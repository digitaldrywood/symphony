package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/digitaldrywood/detent/internal/hubserver"
)

func newHubRecoveryCommands(lookupEnv func(string) string) []*cobra.Command {
	var commands []*cobra.Command
	for _, operation := range []string{"backup", "verify", "restore"} {
		var source, destination, tokenEnv string
		cmd := &cobra.Command{
			Use: operation,
			Short: map[string]string{
				"backup":  "Export a stopped Hub to a new private SQLite snapshot",
				"verify":  "Verify a stopped Hub or snapshot without migrating it",
				"restore": "Import a Hub snapshot with fresh credentials and fenced leases",
			}[operation],
			Example:      "detent hub " + operation + " --database /private/hub.db",
			Args:         NoArgs,
			SilenceUsage: true,
			RunE: func(cmd *cobra.Command, _ []string) error {
				if _, err := OutputForCommand(cmd); err != nil {
					return err
				}
				if strings.TrimSpace(source) == "" {
					return fmt.Errorf("hub %s requires --database", operation)
				}
				if operation != "verify" && strings.TrimSpace(destination) == "" {
					return fmt.Errorf("hub %s requires --output", operation)
				}
				switch operation {
				case "backup":
					return hubserver.BackupDatabase(cmd.Context(), source, destination)
				case "restore":
					if !validEnvName(tokenEnv) {
						return errors.New("invalid administrator token environment variable name")
					}
					result, err := hubserver.RestoreDatabase(cmd.Context(), source, destination, []byte(lookupEnv(tokenEnv)))
					if err != nil {
						return err
					}
					return json.NewEncoder(cmd.OutOrStdout()).Encode(result)
				default:
					result, err := hubserver.VerifyDatabase(cmd.Context(), source)
					if err != nil {
						return err
					}
					return json.NewEncoder(cmd.OutOrStdout()).Encode(result)
				}
			},
		}
		cmd.Flags().StringVar(&source, "database", "", "existing stopped Hub database or snapshot")
		if operation != "verify" {
			cmd.Flags().StringVar(&destination, "output", "", "new destination database; existing paths are never replaced")
		}
		if operation == "restore" {
			cmd.Flags().StringVar(&tokenEnv, "admin-token-env", "DETENT_HUB_ADMIN_TOKEN", "environment variable containing a fresh administrator token")
		}
		commands = append(commands, cmd)
	}
	return commands
}
