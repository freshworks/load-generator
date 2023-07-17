package cmd

import (
	"context"

	"github.com/freshworks/load-generator/internal/loadgen"
	"github.com/freshworks/load-generator/internal/runner"
	"github.com/freshworks/load-generator/internal/smtp"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/spf13/cobra"
)

var smtpCmd = &cobra.Command{
	Use:   "smtp <target>",
	Short: "SMTP load generator",
	Long:  `SMTP load generator`,
	Example: `
lg smtp https://example.com.service/some/path
lg smtp --requestrate 10 http://example.com/some/path
`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		newGenerator := func(id int, requestrate int, concurrency int, ctx context.Context, s *stats.Stats) loadgen.Generator {
			o := smtp.NewOptions()

			o.Target = args[0]
			o.Username = smtpUsername
			o.Password = smtpPassword
			o.From = smtpFrom
			o.To = smtpTo
			o.Subject = smtpSubject
			o.Data = smtpData
			o.Plaintext = smtpPlaintext
			o.Insecure = smtpInsecure
			o.TlsServerName = smtpTlsServerName
			o.RootCAs = smtpRootCAs
			o.DisableConnectionReuse = smtpDisableConnectionReuse

			return smtp.NewGenerator(id, *o, cmd.Context(), requestrate, s)
		}

		runr := runner.New(requestrate, concurrency, warmup, duration, cmd.Context(), stat, newGenerator)
		runr.Run()

		return nil
	},
}

var smtpUsername string
var smtpPassword string
var smtpFrom string
var smtpTo string
var smtpSubject string
var smtpData string
var smtpInsecure bool
var smtpRootCAs []string
var smtpTlsServerName string
var smtpDisableConnectionReuse bool
var smtpPlaintext bool

func init() {
	rootCmd.AddCommand(smtpCmd)

	smtpCmd.Flags().StringVar(&smtpUsername, "username", "", "Username")
	smtpCmd.Flags().StringVar(&smtpPassword, "password", "hello", "Password")
	smtpCmd.Flags().StringVar(&smtpFrom, "from", "", "Mail sender address")
	smtpCmd.Flags().StringVar(&smtpTo, "to", "", "Mail recipient address")
	smtpCmd.Flags().StringVar(&smtpSubject, "subject", "", "Mail subject")
	smtpCmd.Flags().StringVar(&smtpData, "data", "hello", "Mail body")
	smtpCmd.Flags().BoolVar(&smtpInsecure, "insecure", false, "Allow insecure server connections when using SSL")
	smtpCmd.Flags().StringSliceVar(&smtpRootCAs, "rootca", []string{}, "Add root CAs to add to client trust store")
	smtpCmd.Flags().StringVar(&smtpTlsServerName, "tls-server-name", "", "TLS server name to send in ClientHello SNI extension")
	smtpCmd.Flags().BoolVar(&smtpDisableConnectionReuse, "disable-connection-reuse", false, "Disable TCP connection reuse")
	smtpCmd.Flags().BoolVar(&smtpPlaintext, "plaintext", false, "Force plaintext, to TLS")
}
