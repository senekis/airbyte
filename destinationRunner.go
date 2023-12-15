package airbyte

import (
	"io"
	"os"
)

// DestinationRunner acts as an "orchestrator" of sorts to run your destination for you
type DestinationRunner struct {
	w          io.Writer
	dst        Destination
	msgTracker MessageTracker
}

func NewDestinationRunner(dst Destination, w io.Writer) DestinationRunner {
	w = newSafeWriter(w)
	msgTracker := MessageTracker{
		State:  newStateWriter(w),
		Record: newRecordWriter(w),
		Log:    newLogWriter(w),
	}

	return DestinationRunner{
		w:          w,
		dst:        dst,
		msgTracker: msgTracker,
	}
}

func (ds DestinationRunner) Start() error {
	switch cmd(os.Args[1]) {
	case cmdSpec:
		spec, err := ds.dst.Spec(LogTracker{
			Log: ds.msgTracker.Log,
		})
		if err != nil {
			ds.msgTracker.Log(LogLevelError, "failed:"+err.Error())
			return err
		}

		return write(ds.w, &message{
			Type:                   msgTypeSpec,
			ConnectorSpecification: spec,
		})

	case cmdCheck:
		configPath, err := getConnectorConfigPath()
		if err != nil {
			return err
		}

		if err := ds.dst.Check(configPath, LogTracker{
			Log: ds.msgTracker.Log,
		}); err != nil {
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

	case cmdWrite:
		var incat ConfiguredCatalog
		p, err := getCatalogPath()
		if err != nil {
			return err
		}

		if err := UnmarshalFromPath(p, &incat); err != nil {
			return err
		}

		configPath, err := getConnectorConfigPath()
		if err != nil {
			return err
		}

		if err := ds.dst.Write(configPath, &incat, ds.msgTracker); err != nil {
			return err
		}
	}

	return nil
}
