package airbyte

// Destination is the interface you need to define to create your destination!
type Destination interface {
	// Spec returns the input "form" spec needed for your destination
	Spec(logTracker LogTracker) (*ConnectorSpecification, error)

	// Check verifies the destination - usually verify creds/connection etc.
	Check(dstCfgPath string, logTracker LogTracker) error

	// Write will write the actual data from your source to a Propel Data Pool
	Write(dstCfgPath string, configuredCat *ConfiguredCatalog, tracker MessageTracker) error
}
