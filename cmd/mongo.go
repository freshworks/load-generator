package cmd

import (
	"context"

	"github.com/freshworks/load-generator/internal/loadgen"
	"github.com/freshworks/load-generator/internal/mongo"
	"github.com/freshworks/load-generator/internal/runner"
	"github.com/freshworks/load-generator/internal/stats"
	"github.com/spf13/cobra"
)

var (
	mongoDatabase   string
	mongoCollection string
	mongoOperation  string
	mongoDocument   string
	mongoFilter     string
	mongoUpdate     string
	mongoUsername   string
	mongoPassword   string
	mongoAuthDB     string
	mongoTLS        bool
)

var mongoCmd = &cobra.Command{
	Use:   "mongo <connection-string>",
	Short: "MongoDB load generator",
	Long: `MongoDB load generator

Generates MongoDB load using various operations (find, insert, update, delete, aggregate).
It reports metrics by operation type and collection.
`,
	Example: `
# Basic find operation
lg mongo --database mydb --collection users --operation find --filter '{"status":"active"}' mongodb://localhost:27017

# Insert operation
lg mongo --database mydb --collection users --operation insert --document '{"name":"John","age":30}' mongodb://localhost:27017

# Update operation  
lg mongo --database mydb --collection users --operation update --filter '{"name":"John"}' --update '{"$set":{"age":31}}' mongodb://localhost:27017

# With authentication
lg mongo --database mydb --collection users --operation find --username user --password pass --auth-db admin mongodb://localhost:27017

# With TLS
lg mongo --database mydb --collection users --operation find --tls mongodb://localhost:27017
`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		connectionString := "mongodb://localhost:27017"
		if len(args) > 0 {
			connectionString = args[0]
		}

		newGenerator := func(id int, requestrate int, concurrency int, ctx context.Context, s *stats.Stats) loadgen.Generator {
			o := mongo.NewOptions()
			o.ConnectionString = connectionString
			o.Database = mongoDatabase
			o.Collection = mongoCollection
			o.Operation = mongoOperation
			o.Document = mongoDocument
			o.Filter = mongoFilter
			o.Update = mongoUpdate
			o.Username = mongoUsername
			o.Password = mongoPassword
			o.AuthDB = mongoAuthDB
			o.TLS = mongoTLS

			return mongo.NewGenerator(id, *o, ctx, requestrate, s)
		}

		runr := runner.New(requestrate, concurrency, warmup, duration, cmd.Context(), stat, newGenerator)
		runr.Run()

		return nil
	},
}

func init() {
	rootCmd.AddCommand(mongoCmd)
	
	mongoCmd.Flags().StringVar(&mongoDatabase, "database", "test", "MongoDB database name")
	mongoCmd.Flags().StringVar(&mongoCollection, "collection", "test", "MongoDB collection name")
	mongoCmd.Flags().StringVar(&mongoOperation, "operation", "find", "MongoDB operation (find, insert, update, delete, aggregate)")
	mongoCmd.Flags().StringVar(&mongoDocument, "document", "{}", "Document for insert operations (JSON)")
	mongoCmd.Flags().StringVar(&mongoFilter, "filter", "{}", "Filter for find/update/delete operations (JSON)")
	mongoCmd.Flags().StringVar(&mongoUpdate, "update", "{}", "Update document for update operations (JSON)")
	mongoCmd.Flags().StringVar(&mongoUsername, "username", "", "MongoDB username")
	mongoCmd.Flags().StringVar(&mongoPassword, "password", "", "MongoDB password")
	mongoCmd.Flags().StringVar(&mongoAuthDB, "auth-db", "admin", "Authentication database")
	mongoCmd.Flags().BoolVar(&mongoTLS, "tls", false, "Enable TLS connection")
}