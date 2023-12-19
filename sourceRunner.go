package airbyte

import (
	"io"

	"github.com/spf13/cobra"
)

// SourceRunner acts as an "orchestrator" of sorts to run your source for you
type SourceRunner struct {
	w          io.Writer
	src        Source
	msgTracker MessageTracker
}

// NewSourceRunner takes your defined Source and plugs it in with the rest of airbyte
func NewSourceRunner(src Source, w io.Writer) SourceRunner {
	w = newSafeWriter(w)
	msgTracker := MessageTracker{
		Record: newRecordWriter(w),
		State:  newStateWriter(w),
		Log:    newLogWriter(w),
	}

	return SourceRunner{
		w:          w,
		src:        src,
		msgTracker: msgTracker,
	}
}

// Start starts your source
// Example usage would look like this in your main.go
//
//	 func() main {
//		src := newCoolSource()
//		runner := airbyte.NewSourceRunner(src)
//		err := runner.Start()
//		if err != nil {
//			log.Fatal(err)
//		 }
//	 }
//
// Yes, it really is that easy!
func (sr SourceRunner) Start() error {
	cmd := &cobra.Command{
		Use:   "source",
		Short: "Airbyte source",
	}

	cmd.AddCommand(sr.spec())
	cmd.AddCommand(sr.check())
	cmd.AddCommand(sr.discover())
	cmd.AddCommand(sr.read())

	if err := cmd.Execute(); err != nil {
		return err
	}

	return nil
}

func (sr SourceRunner) spec() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "spec",
		Short: "Connector configuration schema",
		RunE: func(cmd *cobra.Command, args []string) error {
			spec, err := sr.src.Spec(LogTracker{
				Log: sr.msgTracker.Log,
			})

			if err != nil {
				sr.msgTracker.Log(LogLevelError, "failed: "+err.Error())
				return err
			}

			return write(sr.w, &message{
				Type:                   msgTypeSpec,
				ConnectorSpecification: spec,
			})
		},
	}

	return cmd
}

func (sr SourceRunner) check() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "check",
		Short: "Validates the given configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := sr.src.Check(configPath, LogTracker{
				Log: sr.msgTracker.Log,
			})

			if err != nil {
				return write(sr.w, &message{
					Type: msgTypeConnectionStat,
					connectionStatus: &connectionStatus{
						Status:  checkStatusFailed,
						Message: err.Error(),
					},
				})
			}

			return write(sr.w, &message{
				Type: msgTypeConnectionStat,
				connectionStatus: &connectionStatus{
					Status: checkStatusSuccess,
				},
			})
		},
	}

	cmd.Flags().StringVar(&configPath, "config", "", "Configuration file")

	cobra.CheckErr(cmd.MarkFlagRequired("config"))

	return cmd
}

func (sr SourceRunner) discover() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "discover",
		Short: "List the available tables",
		RunE: func(cmd *cobra.Command, args []string) error {
			ct, err := sr.src.Discover(configPath, LogTracker{
				Log: sr.msgTracker.Log,
			})

			if err != nil {
				return err
			}

			return write(sr.w, &message{
				Type:    msgTypeCatalog,
				Catalog: ct,
			})
		},
	}

	cmd.Flags().StringVar(&configPath, "config", "", "Configuration file")

	cobra.CheckErr(cmd.MarkFlagRequired("config"))

	return cmd
}

func (sr SourceRunner) read() *cobra.Command {
	var (
		configPath  string
		catalogPath string
		statePath   string
	)

	cmd := &cobra.Command{
		Use:   "read",
		Short: "Extracts data from the underlying data store",
		RunE: func(cmd *cobra.Command, args []string) error {
			var catalog ConfiguredCatalog
			if err := UnmarshalFromPath(catalogPath, &catalog); err != nil {
				return err
			}

			if err := sr.src.Read(configPath, statePath, &catalog, sr.msgTracker); err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&configPath, "config", "", "Configuration file")
	cmd.Flags().StringVar(&catalogPath, "catalog", "", "Catalog file")
	cmd.Flags().StringVar(&statePath, "state", "", "State file")

	cobra.CheckErr(cmd.MarkFlagRequired("config"))
	cobra.CheckErr(cmd.MarkFlagRequired("catalog"))
	cobra.CheckErr(cmd.MarkFlagRequired("state"))

	return cmd
}
