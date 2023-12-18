package airbyte

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/bitstrapped/airbyte/schema"
)

// InferSchemaFromStruct Infer schema translates golang structs to JSONSchema format
func InferSchemaFromStruct(i any, logTracker LogTracker) Properties {
	var prop Properties

	s, err := schema.Generate(reflect.TypeOf(i))
	if err != nil {
		logTracker.Log(LogLevelError, fmt.Sprintf("generate schema error: %v", err))
		return prop
	}

	b, err := json.Marshal(s)
	if err != nil {
		logTracker.Log(LogLevelError, fmt.Sprintf("json marshal schema error: %v", err))
		return prop
	}

	err = json.Unmarshal(b, &prop)
	if err != nil {
		logTracker.Log(LogLevelError, fmt.Sprintf("unmarshal schema to propspec error: %v", err))
		return prop
	}

	return prop
}
