package psql

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/freshworks/load-generator/internal/stats"
	"github.com/freshworks/load-generator/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/ory/dockertest/v3"
	mysqlquery "github.com/percona/go-mysql/query"
)

const (
	// container repository
	postgresDockerRepo = "postgres"
	postgresPort       = "5432"
)

// starts postgres db with given version as a docker container
// returns connection string to the database and a cleanup function to clear resources.

func RunPostgresDB(version string, certData *Cert) (string /*DB Connection String*/, func() /* cleanup function to clear resources */, error) {
	sslEnabled := (certData != nil)
	connString, cleanup, err := runPostgresDB(version, certData)
	if sslEnabled {
		connString = fmt.Sprintf("%v?sslmode=verify-ca&sslrootcert=%v&sslcert=%v&sslkey=%v", connString, certData.CACertPath, certData.CACertPath, certData.privateKeyPath)
	}
	return connString, cleanup, err
}

// for testingonly; for normal usecases, use RunPostgresDB
func RunPostgresDBWithWrongCerts(version string, certData *Cert) (string /*DB Connection String*/, func() /* cleanup function to clear resources */, error) {
	sslEnabled := (certData != nil)
	wrongCert := &Cert{}
	wrongCert.PopulateSelfSigned("fakeOrganization", "fake-domain.com")
	connString, cleanup, err := runPostgresDB(version, wrongCert)
	if sslEnabled {
		connString = fmt.Sprintf("%v?sslmode=verify-ca&sslrootcert=%v&sslcert=%v&sslkey=%v", connString, certData.CACertPath, certData.CACertPath, certData.privateKeyPath)
	}
	return connString, cleanup, err
}

func runPostgresDB(version string, certData *Cert) (string /*DB Connection String*/, func() /* cleanup function to clear resources */, error) {
	var connString string
	var mountOptions []string
	sslEnabled := (certData != nil)
	envOptions := getDockerPSQLOptions("localhost", postgresPort, "loadgen", "loadgen")
	if sslEnabled {

		mountOptions = getPSQLDockerVolumeOptions(certData)
	}

	entrypointOverrides := []string{
		`docker-entrypoint.sh`,
		"postgres",
	}
	if sslEnabled {
		entrypointOverrides = []string{
			"bash",
			"-c",
			// during mounting - getting uid, gid and permissions setting
			// right is important for postgres server to boot up properly.
			// below steps work well both in mac dev enviroment and
			// docker-in-docker linux environment.
			`
			mkdir /certs
			cp /tmp/server.crt /certs/server-copy.crt
			cp /tmp/server.key /certs/server-copy.key
			chown postgres:postgres /certs/server-copy.crt
			chown postgres:postgres /certs/server-copy.key
			chmod 600 /certs/server-copy.crt
			chmod 600 /certs/server-copy.key
			docker-entrypoint.sh postgres -c log_min_messages=DEBUG5 -c log_connections=true \
			-c ssl=on -c ssl_cert_file=/certs/server-copy.crt -c ssl_key_file=/certs/server-copy.key`,
		}
	}

	resource, pool, err := runDockerContainer(
		postgresDockerRepo,
		version,
		mountOptions,
		envOptions,
		[]string{},
		entrypointOverrides,
	)

	// construct cleanup function
	cleanup := func() {
		// uncomment below for debugging with container logs

		// ctx := context.Background()
		// cli, _ := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
		// containerID := resource.Container.ID
		// out, _ := cli.ContainerLogs(ctx, containerID, types.ContainerLogsOptions{ShowStdout: true})
		// bytes, _ := io.ReadAll(out)
		//
		// fmt.Printf("\n\ncontainer logs: %v\n\n", string(bytes))

		pool.Purge(resource)
	}
	// if function panics anywhere in further steps,
	// cleanup resources.
	defer func() {
		if nil != recover() {
			cleanup()
		}
	}()

	pool.MaxWait = 1 * time.Minute // set retry deadline for psql availability
	if err != nil {
		cleanup()
		return "", nil, err
	}

	// construct connection string
	port := resource.GetPort(postgresPort + "/tcp")
	connString = fmt.Sprintf("postgresql://%v:%v@127.0.0.1:%v/postgres",
		"loadgen",
		"loadgen",
		port,
	)

	// check if postgres is ready before returning the connection string
	if err := pool.Retry(func() error {
		connConfig, err := pgx.ParseConfig(connString)
		if err != nil {
			return err
		}

		connId := stdlib.RegisterConnConfig(connConfig)

		db, err := sql.Open("psql", connId)
		if err != nil {
			return err
		}
		_, err = db.Query("select 1")

		return err
	}); err != nil {
		cleanup()
		return "", nil, err
	}

	return connString, cleanup, nil
}

func runDockerContainer(repo string, tag string, mountOptions []string, envOptions []string, cmdOptions []string, entrypointOverrides []string) (*dockertest.Resource /* returning pool for the convinience of retry backoff */, *dockertest.Pool, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, nil, err
	}

	o := &dockertest.RunOptions{
		Repository: repo,
		Tag:        tag,
		Mounts:     mountOptions,
		Env:        envOptions,
		Cmd:        cmdOptions,
		Entrypoint: entrypointOverrides,
	}
	resource, err := pool.RunWithOptions(o)

	return resource, pool, err
}

func getDockerPSQLOptions(host string, port string, username string, password string) /* env */ []string {

	env := []string{
		"POSTGRES_USER=" + username,
		"POSTGRES_PASSWORD=" + password,
	}

	return env
}

func getPSQLDockerVolumeOptions(certData *Cert) []string {
	var mountOptions []string

	certMountOptions := []string{
		certData.publicKeyPath + ":/tmp/server.crt",
		certData.privateKeyPath + ":/tmp/server.key",
	}

	mountOptions = append(mountOptions, certMountOptions...)
	return mountOptions
}

func getStatResultFor(s *stats.Stats, key string, query string) *stats.Result {
	subkey := mysqlquery.Id(mysqlquery.Fingerprint(query))

	r := s.Export()

	for _, rr := range r.Results {
		if key == rr.Target && subkey == rr.SubTarget {
			return &rr
		}
	}

	return nil
}

type Cert struct {
	publicKey      []byte
	privatetKey    []byte
	caCert         []byte
	publicKeyPath  string
	privateKeyPath string
	CACertPath     string
}

// remove temp files
func (c *Cert) Cleanup() {
	os.Remove(c.CACertPath)
	os.Remove(c.publicKeyPath)
	os.Remove(c.privateKeyPath)
}

// generate and populate self signed certificates in struct fields.
func (c *Cert) PopulateSelfSigned(organization string, dnsName string) error {

	ca, pk, err := utils.GenCert(organization, dnsName)
	if err != nil {
		return err
	}
	c.caCert = ca
	c.CACertPath, err = utils.GetTempFile("testdata", ca)
	if err != nil {
		return err
	}
	c.publicKey = ca
	c.publicKeyPath = c.CACertPath
	if err != nil {
		return err
	}
	c.privatetKey = pk
	c.privateKeyPath, err = utils.GetTempFile("testdata", pk)

	if err != nil {
		return err
	}
	return nil
}
