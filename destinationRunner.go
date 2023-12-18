package airbyte

import (
	"io"

	"github.com/spf13/cobra"
)

// DestinationRunner acts as an "orchestrator" of sorts to run your destination for you
type DestinationRunner struct {
	r          io.Reader
	w          io.Writer
	dst        Destination
	msgTracker MessageTracker
}

func NewDestinationRunner(dst Destination, r io.Reader, w io.Writer) DestinationRunner {
	w = newSafeWriter(w)
	msgTracker := MessageTracker{
		State:  newStateWriter(w),
		Record: newRecordWriter(w),
		Log:    newLogWriter(w),
	}

	return DestinationRunner{
		r:          r,
		w:          w,
		dst:        dst,
		msgTracker: msgTracker,
	}
}

func (ds DestinationRunner) Start() error {
	cmd := &cobra.Command{
		Use:   "destination",
		Short: "Airbyte destination",
	}

	cmd.AddCommand(ds.spec())
	cmd.AddCommand(ds.check())
	cmd.AddCommand(ds.write())

	if err := cmd.Execute(); err != nil {
		return err
	}

	return nil
}

func (ds DestinationRunner) spec() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "spec",
		Short: "Connector configuration schema",
		RunE: func(cmd *cobra.Command, args []string) error {
			spec, err := ds.dst.Spec(LogTracker{
				Log: ds.msgTracker.Log,
			})

			if err != nil {
				ds.msgTracker.Log(LogLevelError, "failed: "+err.Error())
				return err
			}

			return write(ds.w, &message{
				Type:                   msgTypeSpec,
				ConnectorSpecification: spec,
			})
		},
	}

	return cmd
}

func (ds DestinationRunner) check() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "check",
		Args:  cobra.ExactArgs(1),
		Short: "Validates the given configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := ds.dst.Check(configPath, LogTracker{
				Log: ds.msgTracker.Log,
			})

			if err != nil {
				return write(ds.w, &message{
					Type: msgTypeConnectionStat,
					connectionStatus: &connectionStatus{
						Status:  checkStatusFailed,
						Message: err.Error(),
					},
				})
			}

			return write(ds.w, &message{
				Type: msgTypeConnectionStat,
				connectionStatus: &connectionStatus{
					Status: checkStatusSuccess,
				},
			})
		},
	}

	cmd.Flags().StringVar(&configPath, "config", "", "Configuration file")

	return cmd
}

func (ds DestinationRunner) write() *cobra.Command {
	var configPath string
	var catalogPath string

	cmd := &cobra.Command{
		Use:   "write",
		Args:  cobra.ExactArgs(2),
		Short: "Write records at destination",
		RunE: func(cmd *cobra.Command, args []string) error {
			var catalog ConfiguredCatalog
			if err := UnmarshalFromPath(catalogPath, &catalog); err != nil {
				return err
			}

			err := ds.dst.Write(configPath, &catalog, ds.r, LogTracker{
				Log: ds.msgTracker.Log,
			})

			if err != nil {
				return write(ds.w, &message{
					Type: msgTypeConnectionStat,
					connectionStatus: &connectionStatus{
						Status:  checkStatusFailed,
						Message: err.Error(),
					},
				})
			}

			return write(ds.w, &message{
				Type: msgTypeConnectionStat,
				connectionStatus: &connectionStatus{
					Status: checkStatusSuccess,
				},
			})
		},
	}

	cmd.Flags().StringVar(&configPath, "config", "", "Configuration file")
	cmd.Flags().StringVar(&catalogPath, "catalog", "", "Catalog file")

	return cmd
}
