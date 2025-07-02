An generic load test utility to generate load for various endpoints.

Following endpoints are supported:

  * HTTP
  * gRPC
  * Redis
  * MySQL
  * PostgreSQL
  * Cassandra
  * SMTP
  * Kafka

Features:
  * Metrics: Latency distribution (percentiles) and response time histograms
    are reported (using HdrHistogram)
  * Programmable via Lua: you can mix/match any of these endpoints and
    metrics will be collected and reported for each of them
  * Custom metrics ("make two gRPC calls and one MySQL, report total for
    these three calls combined as single unit" etc)
  * SQL metrics: queries will be finger printed and metrics are computed on
    finger printed signatures

## Installation
You can download from the [Release](https://github.com/freshworks/load-generator/releases/latest)

Or Compile from source

Requires go

```
make
```



## Usage

It can be invoked directly for simple test cases or create a Lua file to do
more complex operations.

### HTTP

Generate http load:

`lg http --duration 10s  --concurrency 1 --requestrate 1 http://google.com`

<details>
  <summary>Output:</summary>

  ```
INFO[2019-10-17 18:09:57.334] Starting ...                                  Id=0
INFO[2019-10-17 18:10:00.334] Warmup done (2 seconds)                       Id=0

http://google.com:
+-----+--------+--------+--------+--------+--------+--------+--------+--------+------+--------+-----+-----+-----+-----+
| URL |  AVG   | STDDEV |  MIN   |  MAX   |  P50   |  P95   |  P99   | P99.99 | TOTAL| AVGRPS | 2XX | 3XX | 4XX | 5XX |
+-----+--------+--------+--------+--------+--------+--------+--------+--------+------+--------+-----+-----+-----+-----+
| /   | 134.10 |  10.55 | 125.06 | 164.48 | 130.30 | 164.48 | 164.48 | 164.48 |    20|   1.00 |   0 |  10 |   0 |   0 |
+-----+--------+--------+--------+--------+--------+--------+--------+--------+------+--------+-----+-----+-----+-----+

Response time histogram (ms):

/:
   125.056 [         1] |■■■■■■■■
   128.998 [         1] |■■■■■■■■
   132.940 [         5] |■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
   136.882 [         1] |■■■■■■■■
   140.824 [         1] |■■■■■■■■
   144.766 [         0] |
   148.708 [         0] |
   152.650 [         0] |
   156.592 [         0] |
   160.534 [         0] |
   164.479 [         1] |■■■■■■■■
  ```
</details>

See see [here](scripts/test.lua) on how to do this via Lua script

### gRPC
Generate gRPC load. It expects gRPC method and payload(as json, which
will get converted to protobuf) as input.

1. Find gRPC method name and generate corresponding payload:

   There are two options:

   - Use server reflection (preferred):
     ```
     lg grpc --template \
        --plaintext grpcb.in:9000
     ```

     <details>
       <summary>Output:</summary>

       ```

       ========= hello.HelloService =========

       --method 'hello.HelloService.SayHello'
       --data '{"greeting":""}'

       --method 'hello.HelloService.LotsOfReplies'
       --data '{"greeting":""}'

       --method 'hello.HelloService.LotsOfGreetings'
       --data '{"greeting":""}'

       --method 'hello.HelloService.BidiHello'
       --data '{"greeting":""}'
       ```

     </details>

   - Use .proto files:
     ```
     lg grpc --template \
        --proto ~/go/src/google.golang.org/grpc/examples/helloworld/helloworld/helloworld.proto \
        --import-path ~/go/src/google.golang.org/grpc/examples/helloworld/helloworld/
     ```

     <details>
       <summary>Output:</summary>

       ```

     ========= helloworld.Greeter =========

     --method 'helloworld.Greeter.SayHello'
     --data '{"name":""}'
       ```
     </details>

2. Then, we can generate load:

```
lg grpc --requestrate 1 --concurrency 1 --duration 10s \
    --method 'hello.HelloService.SayHello' --data '{"greeting":"meow"}' \
    --plaintext grpcb.in:9000
```

<details>
  <summary>Output:</summary>

  ```
INFO[0000] Starting ...
INFO[0005] Warmup done (5s seconds)


GRPC Metrics:

grpcb.in:9000:
+-----------------------------+--------+--------+--------+--------+--------+--------+--------+--------+-------+--------+--------+----------+
|           METHOD            |  AVG   | STDDEV |  MIN   |  MAX   |  P50   |  P95   |  P99   | P99.99 | TOTAL | AVGRPS | ERRORS | DEADLINE |
+-----------------------------+--------+--------+--------+--------+--------+--------+--------+--------+-------+--------+--------+----------+
| hello.HelloService.SayHello | 155.61 |   0.34 | 155.14 | 156.29 | 155.65 | 156.29 | 156.29 | 156.29 |    10 |   1.00 |      0 |        0 |
+-----------------------------+--------+--------+--------+--------+--------+--------+--------+--------+-------+--------+--------+----------+

Response time histogram (ms):

hello.HelloService.SayHello:
   155.136 [         2] |■■■■■■■■■■■■■■■■■■■■■■■■■■■
   155.251 [         2] |■■■■■■■■■■■■■■■■■■■■■■■■■■■
   155.481 [         1] |■■■■■■■■■■■■■
   155.596 [         3] |■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
   155.941 [         1] |■■■■■■■■■■■■■
   156.287 [         1] |■■■■■■■■■■■■■
  ```
  </details>

See see [here](scripts/grpc.lua) how to do this via Lua script

### MySQL
Generate MySQL load:

`lg mysql --duration 10s --requestrate 0 --query 'SELECT 1' 'root:@tcp(127.0.0.1:3306)/'`
<details>
  <summary>Output:</summary>

  ```
 INFO[2019-10-17 18:22:07.232] Starting ...                                  Id=0
INFO[2019-10-17 18:22:10.233] Warmup done (2 seconds)                        Id=0

:
+------------------+------+--------+-------+-------+-------+-------+-------+--------+-------+----------+
|      QUERY       | AVG  | STDDEV |  MIN  |  MAX  |  P50  |  P95  |  P99  | P99.99 | TOTAL |  AVGRPS  |
+------------------+------+--------+-------+-------+-------+-------+-------+--------+-------+----------+
| 16219655761820A2 |0.032 |  0.007 | 0.026 | 2.025 | 0.031 | 0.040 | 0.053 |  0.180 |268889 | 26884.80 |
+------------------+------+--------+-------+-------+-------+-------+-------+--------+-------+----------+

Response time histogram (ms):

16219655761820A2:
     0.026 [       116] |
     0.225 [    268754] |■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
     0.424 [         9] |
     0.623 [         6] |
     0.822 [         2] |
     1.021 [         0] |
     1.220 [         1] |
     1.419 [         0] |
     1.618 [         0] |
     1.817 [         0] |
     2.025 [         1] |

Digest to query mapping:
  16219655761820A2 : select ?
  ```
</details>

See [here](scripts/sql.lua) how to do this via Lua script.

### Postgres
Generate PSQL load:

`./lg psql --duration 10s --requestrate 1 --query "SELECT 1"  "postgresql://postgres@localhost:5432/postgres"`
<details>
  <summary>Output:</summary>

  ```
./lg  psql --query "select 1" --requestrate 1 --duration 10s "postgresql://postgres@127.0.0.1:5432/"


PostgresQL Metrics:

127.0.0.1:
+------------------+------+--------+------+------+------+------+------+--------+-------+--------+--------+
|      QUERY       | AVG  | STDDEV | MIN  | MAX  | P50  | P95  | P99  | P99.99 | TOTAL | AVGRPS | ERRORS |
+------------------+------+--------+------+------+------+------+------+--------+-------+--------+--------+
| 16219655761820A2 | 0.70 |   0.27 | 0.25 | 1.00 | 0.81 | 1.00 | 1.00 |   1.00 |    10 |   1.00 |      0 |
+------------------+------+--------+------+------+------+------+------+--------+-------+--------+--------+

Response time histogram (ms):

16219655761820A2:
     0.252 [         2] |■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
     0.326 [         1] |■■■■■■■■■■■■■■■■■■■■
     0.622 [         1] |■■■■■■■■■■■■■■■■■■■■
     0.770 [         1] |■■■■■■■■■■■■■■■■■■■■
     0.844 [         2] |■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
     0.918 [         2] |■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
     0.997 [         1] |■■■■■■■■■■■■■■■■■■■■
Digest to query mapping:
  16219655761820A2 : select ?
  ```
</details>

See [here](scripts/psql.lua) how to do this via Lua script.

### CQL
Generate Cassandra load:

`lg cql --requestrate 1 cql --disable-peers-lookup --username user --password password --plaintext --query 'select * from system.peers LIMIT 1' localhost:9042`

<details>
  <summary>Output:</summary>

  ```
Cassandra Metrics:

:
+------------------+------+--------+------+------+------+------+------+--------+-------+--------+--------+
|      QUERY       | AVG  | STDDEV | MIN  | MAX  | P50  | P95  | P99  | P99.99 | TOTAL | AVGRPS | ERRORS |
+------------------+------+--------+------+------+------+------+------+--------+-------+--------+--------+
| 818FE829A3B44923 | 1.43 |   0.08 | 1.28 | 1.56 | 1.42 | 1.56 | 1.56 |   1.56 |     8 |   1.00 |      0 |
+------------------+------+--------+------+------+------+------+------+--------+-------+--------+--------+

Response time histogram (ms):

818FE829A3B44923:
     1.284 [         1] |■■■■■■■■■■■■■■■■■■■■
     1.311 [         1] |■■■■■■■■■■■■■■■■■■■■
     1.392 [         2] |■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
     1.446 [         1] |■■■■■■■■■■■■■■■■■■■■
     1.473 [         2] |■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
     1.558 [         1] |■■■■■■■■■■■■■■■■■■■■

  ```
</details>

See see [here](scripts/cql.lua) how to do this via Lua script.

Also see [here](scripts/ucr/ucr.lua) on how we are doing full
application load test on database (by making CQL calls for given business
logic) and report low level CQL metrics and as well as high level meaningful
metrics.

### Redis
Generate Redis load:

```
lg --duration 20s --requestrate 0 --redis --redis-cmd 'get' --redis-arg hello 127.0.0.1:6379
```
<details>
  <summary>Output:</summary>

  ```
  INFO[2019-10-17 18:34:04.411] Starting ...                                  Id=0
INFO[2019-10-17 18:34:07.411] Warmup done (2 seconds)                         Id=0

:
+-------+------+--------+------+------+------+------+------+--------+-------+----------+--------+
| QUERY | AVG  | STDDEV | MIN  | MAX  | P50  | P95  | P99  | P99.99 | TOTAL |  AVGRPS  | ERRORS |
+-------+------+--------+------+------+------+------+------+--------+-------+----------+--------+
| get   | 0.02 |  0.01  | 0.02 | 3.17 | 0.02 | 0.03 | 0.04 |  0.12  |773807 | 38691.20 |      0 |
+-------+------+--------+------+------+------+------+------+--------+-------+----------+--------+

Response time histogram (ms):

get:
     0.018 [         3] |
     0.333 [    773783] |■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
     0.648 [         9] |
     0.963 [         4] |
     1.278 [         3] |
     1.593 [         2] |
     1.908 [         1] |
     2.223 [         1] |
     2.538 [         0] |
     2.853 [         0] |
     3.169 [         1] |
  ```
  </details>
See see [here](scripts/test.lua) how to do this via Lua script

### SMTP
Generate SMTP:

`lg smtp --from bar@bar.com --to foo@foo.com --username username --password password "127.0.0.1:1025" --plaintext`

<details>
  <summary>Output:</summary>

  ```
SMTP Metrics:

localhost:1025:
+----------------+------+--------+------+------+------+------+------+--------+-------+--------+--------+
|      KEY       | AVG  | STDDEV | MIN  | MAX  | P50  | P95  | P99  | P99.99 | TOTAL | AVGRPS | ERRORS |
+----------------+------+--------+------+------+------+------+------+--------+-------+--------+--------+
| localhost:1025 | 1.23 |   0.68 | 0.23 | 1.92 | 1.68 | 1.92 | 1.92 |   1.92 |     9 |   1.00 |      0 |
+----------------+------+--------+------+------+------+------+------+--------+-------+--------+--------+

Response time histogram (ms):

localhost:1025:
     0.225 [         1] |■■■■■■■■■■■■■
     0.394 [         2] |■■■■■■■■■■■■■■■■■■■■■■■■■■■
     0.563 [         1] |■■■■■■■■■■■■■
     1.577 [         1] |■■■■■■■■■■■■■
     1.746 [         3] |■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
     1.918 [         1] |■■■■■■■■■■■■■

  ```
</details>

See see [here](scripts/smtp.lua) how to do this via Lua script.

### Kafka
Generate Kafka load by producing messages to or consuming messages from a Kafka topic:

```
# Produce messages to a Kafka topic
lg kafka --brokers "localhost:9092" --topic "test-topic" --message "Hello World" --requestrate 10 --duration 30s

# Consume messages from a Kafka topic
lg kafka --brokers "localhost:9092" --topic "test-topic" --group "consumer-group-1" --read --requestrate 10 --duration 30s
```

#### SCRAM Authentication Support

The load generator supports SASL SCRAM authentication for Kafka connections. Both SCRAM-SHA-256 and SCRAM-SHA-512 mechanisms are supported, with or without TLS.

```bash
# Using SCRAM authentication with TLS
lg kafka --brokers "kafka-broker:9092" --topic "test-topic" --message "Hello World" \
  --username "user" --password "password" --sasl-mechanism "SCRAM-SHA-512" --tls \
  --requestrate 10 --duration 30s

# Using SCRAM authentication without TLS
lg kafka --brokers "kafka-broker:9092" --topic "test-topic" --message "Hello World" \
  --username "user" --password "password" --sasl-mechanism "SCRAM-SHA-256" \
  --requestrate 10 --duration 30s
```

#### Authentication Options

| Option | Description |
|--------|-------------|
| `--username` | SASL username for authentication |
| `--password` | SASL password for authentication |
| `--sasl-mechanism` | SASL mechanism (SCRAM-SHA-256 or SCRAM-SHA-512, default: SCRAM-SHA-512) |
| `--tls` | Enable TLS for Kafka connections (optional with SCRAM authentication) |

#### Testing with Docker Compose

The repository includes a Docker Compose configuration with both a regular Kafka instance and a Kafka instance with SCRAM authentication:

```bash
# Start Kafka with SCRAM authentication
docker compose up -d zookeeper kafka-scram
```

##### Creating a Test Topic

To create a test topic in the SCRAM-enabled Kafka service:

```bash
# Create a test topic with SCRAM authentication
docker compose exec kafka-scram bash -c "echo 'security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"admin-secret\";' > /tmp/client.properties && \
kafka-topics.sh --create --topic scram-test-topic --bootstrap-server localhost:9093 --command-config /tmp/client.properties --partitions 1 --replication-factor 1"
```

##### Testing with the Load Generator

```bash
# Test with SCRAM-SHA-512 authentication
lg kafka --brokers "localhost:9093" --topic "scram-test-topic" --message "Hello World" \
  --username "user" --password "user-secret" --sasl-mechanism "SCRAM-SHA-512" \
  --requestrate 10 --duration 30s

# Test with SCRAM-SHA-256 authentication
lg kafka --brokers "localhost:9093" --topic "scram-test-topic" --message "Hello World" \
  --username "user" --password "user-secret" --sasl-mechanism "SCRAM-SHA-256" \
  --requestrate 10 --duration 30s
```

##### Predefined Users

The Kafka SCRAM service comes with two predefined users:

- **Client User**: Username: `user`, Password: `user-secret`
  - Use this for general client operations with the load generator

- **Admin User**: Username: `admin`, Password: `admin-secret`
  - Use this for administrative operations like creating topics

The Kafka SCRAM service is configured to support both SCRAM-SHA-256 and SCRAM-SHA-512 authentication mechanisms.

<details>
  <summary>Output:</summary>

  ```
INFO[0000] Starting ...
INFO[0005] Warmup done (5s seconds)

Kafka Metrics:

localhost:9092:
+------------------+------+--------+------+------+------+------+------+--------+-------+--------+--------+
|       KEY        | AVG  | STDDEV | MIN  | MAX  | P50  | P95  | P99  | P99.99 | TOTAL | AVGRPS | ERRORS |
+------------------+------+--------+------+------+------+------+------+--------+-------+--------+--------+
| write:test-topic | 1.25 |   0.35 | 0.85 | 2.15 | 1.20 | 1.85 | 2.15 |   2.15 |   300 |  10.00 |      0 |
+------------------+------+--------+------+------+------+------+------+--------+-------+--------+--------+

Response time histogram (ms):

write:test-topic:
     0.850 [        15] |■■■■■■■■■■■
     0.980 [        42] |■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
     1.110 [        78] |■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
     1.240 [        69] |■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
     1.370 [        41] |■■■■■■■■■■■■■■■■■■■■■■■■■■■
     1.500 [        25] |■■■■■■■■■■■■■■■■
     1.630 [        15] |■■■■■■■■■■■
     1.760 [         8] |■■■■■
     1.890 [         4] |■■
     2.020 [         2] |■
     2.150 [         1] |■
  ```
</details>

### Lua

> **_NOTE:_** Cookies are retained automatically in http client object (by
> wathing out for set-cookie response headers), so just "logging-in" once
> should be sufficient, the cookies will be automatically sent in the future
> http requests automatically.

Generate load using Lua (script can make use of all supported
grpc/http/redis/mysql etc modules), see [here](scripts/) for examples.

```
lg script --duration 10s --requestrate 1 ./scripts/http.lua
```
<details>
  <summary>Output:</summary>

  ```
INFO[2019-10-17 18:35:19.367] Global called                                 Id=1
INFO[2019-10-17 18:35:19.367] Initializing ...                              Id=1
INFO[2019-10-17 18:35:19.367] Request rate: 1                               Id=1
INFO[2019-10-17 18:35:19.368] Scripts args: <nil>                           Id=1
INFO[2019-10-17 18:35:19.368] Initialization done, took 168.029µs           Id=1
INFO[2019-10-17 18:35:19.368] Starting ...                                  Id=0
INFO[2019-10-17 18:35:20.731] 301 Moved Permanently                         Id=1
INFO[2019-10-17 18:35:21.542] 301 Moved Permanently                         Id=1
INFO[2019-10-17 18:35:22.368] Warmup done (2 seconds)                       Id=0
INFO[2019-10-17 18:35:22.548] 301 Moved Permanently                         Id=1
INFO[2019-10-17 18:35:23.567] 301 Moved Permanently                         Id=1
INFO[2019-10-17 18:35:24.549] 301 Moved Permanently                         Id=1
INFO[2019-10-17 18:35:25.544] 301 Moved Permanently                         Id=1
INFO[2019-10-17 18:35:26.548] 301 Moved Permanently                         Id=1
INFO[2019-10-17 18:35:27.560] 301 Moved Permanently                         Id=1
INFO[2019-10-17 18:35:28.540] 301 Moved Permanently                         Id=1
INFO[2019-10-17 18:35:29.539] 301 Moved Permanently                         Id=1
INFO[2019-10-17 18:35:30.549] 301 Moved Permanently                         Id=1
INFO[2019-10-17 18:35:31.550] 301 Moved Permanently                         Id=1

http://google.com:
+-----+--------+--------+--------+--------+--------+--------+--------+--------+------+--------+-----+-----+-----+-----+
| URL | AVG    | STDDEV |  MIN   |  MAX   |  P50   |  P95   |  P99   | P99.99 | TOTAL| AVGRPS | 2XX | 3XX | 4XX | 5XX |
+-----+--------+--------+--------+--------+--------+--------+--------+--------+------+--------+-----+-----+-----+-----+
| /   | 181.59 |   8.15 | 171.39 | 199.55 | 180.22 | 199.55 | 199.55 | 199.55 |   10 |   1.00 |   0 |  10 |   0 |   0 |
+-----+--------+--------+--------+--------+--------+--------+--------+--------+------+--------+-----+-----+-----+-----+

Response time histogram (ms):

/:
   171.392 [         1] |■■■■■■■■■■
   174.207 [         1] |■■■■■■■■■■
   177.022 [         1] |■■■■■■■■■■
   179.837 [         1] |■■■■■■■■■■
   182.652 [         4] |■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
   185.467 [         0] |
   188.282 [         0] |
   191.097 [         0] |
   193.912 [         1] |■■■■■■■■■■
   196.727 [         0] |
   199.551 [         1] |■■■■■■■■■■
  ```
</details>

A script that combines multiple kinds of workload is possible as well (doing
http call, grpc call, redis call, mysql call etc). They all can be combined
together to mimic real app and metrics can be tracked individually as well
as overall. See [here](scripts/multiple.lua) for an example.

### Server/Client

In this mode, server instance of lg just runs in listen mode. Multiple lg
clients can be run independently (to distribute the load over N
machines). The server can receive metrics from all the clients, aggregate
them and publish as single report. In addition to this, server also exposes
UI for viewing reports (cli table output, json export, latency graphs).

Run the server (localhost example, but can be remote as well):

```
lg server :1234

```

Run the client(s):

```
lg --duration 5s --warmup 1s --server :1234 http http://google.com
```

To view the UI, visit http://localhost:1234

For example, visiting http://localhost:1234/graphs, should show something
like this:

![Latency Graph](latency_graph.png)

## Docker Compose for Testing

A Docker Compose configuration is included to easily spin up services for testing the load generator.

### Available Services

The docker-compose.yml file includes the following services:

- **Kafka & Zookeeper**: For testing Kafka producer and consumer functionality
- **Redis**: For testing Redis commands
- **PostgreSQL**: For testing PostgreSQL queries
- **MySQL**: For testing MySQL queries
- **Cassandra**: For testing CQL queries
- **MailHog**: A development SMTP server with web interface for testing SMTP functionality

### Usage

#### Starting All Services

To start all services:

```bash
docker compose up -d
```

#### Starting a Specific Service

To start a specific service (and its dependencies):

```bash
docker compose up -d <service-name>
```

For example, to start only Kafka and its dependency (Zookeeper):

```bash
docker compose up -d kafka
```

#### Stopping Services

To stop all services:

```bash
docker compose down
```

### Service Connection Details

Refer to each command section above for examples of how to connect to each service.

#### Creating Test Topics in Kafka

To create a test topic in Kafka, you can use the following command:

```bash
docker exec -it kafka kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

To list topics:

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```
