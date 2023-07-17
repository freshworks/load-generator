package utils

import (
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/require"
)

// A test for self signed cert generation,
// because other tests depends on this functionality.
func TestUtils(t *testing.T) {
	require := require.New(t)
	t.Run("TestSelfSignedCertValidity", func(t *testing.T) {
		org := "test"
		hostName := "testing.com"
		ca, _, err := GenCert(org, hostName)
		require.Nil(err)

		// parse and check if returned certificate is correct or not
		caPem, _ := pem.Decode(ca)
		require.NotNil(caPem) // caPem will be empty if pem key is malformed

		caCert, err := x509.ParseCertificate(caPem.Bytes)
		require.Nil(err)

		x509.NewCertPool().AppendCertsFromPEM(ca)

		// test if it really is self signed?
		rootCert := x509.NewCertPool()
		ok := rootCert.AppendCertsFromPEM(ca)
		require.True(ok)

		verifyOpts := x509.VerifyOptions{
			DNSName: hostName,
			Roots:   rootCert,
		}

		_, err = caCert.Verify(verifyOpts)
		require.Nil(err)

	})
}
