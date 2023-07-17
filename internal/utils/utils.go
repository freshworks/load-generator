package utils

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"time"
)

// mostly used for testing
func GenCert(organization string, dnsName string) (
	/* certificate authority */ []byte /* private key  */, []byte, error) {
	// Create a Certificate Authority Cert
	ca := &x509.Certificate{
		Subject: pkix.Name{
			Organization: []string{organization},
		},
		DNSNames:              []string{dnsName},
		SerialNumber:          big.NewInt(22222),
		NotAfter:              time.Now().Add(1 * time.Hour),
		NotBefore:             time.Now(),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// Create a Private Key
	key, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, fmt.Errorf("could not generate rsa key - %s", err)
	}

	// Use CA Cert to sign a CSR and create a Public Cert
	csr := &key.PublicKey
	cert, err := x509.CreateCertificate(rand.Reader, ca, ca, csr, key)
	if err != nil {
		return nil, nil, fmt.Errorf("could not generate certificate - %s", err)
	}

	// Convert keys into pem.Block
	c := &pem.Block{Type: "CERTIFICATE", Bytes: cert}
	k := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}

	return pem.EncodeToMemory(c), pem.EncodeToMemory(k), nil
}

// takes directory name (string) and content of the file (bytes) as parameter
// and returns temp filename and error, if any.
func GetTempFile(name string, content []byte) (string, error) {
	tmpfile, err := ioutil.TempFile("", name)
	if err != nil {
		return "", err
	}
	_, err = tmpfile.Write(content)
	if err != nil {
		return "", err
	}

	err = tmpfile.Close()
	if err != nil {
		return "", err
	}

	return tmpfile.Name(), nil
}
