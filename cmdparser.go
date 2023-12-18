package airbyte

import (
	"encoding/json"
	"os"
)

// UnmarshalFromPath is used to unmarshal json files into respective struct's
// this is most commonly used to unmarshal your State between runs and also unmarshal SourceConfig's
//
// Example usage
//
//	 type CustomState struct {
//		 Timestamp int    `json:"timestamp"`
//		 Foobar    string `json:"foobar"`
//	 }
//
//	 func (s *CustomSource) Read(stPath string, ...) error {
//		 var cs CustomState
//		 err = airbyte.UnmarshalFromPath(stPath, &cs)
//		 if err != nil {
//			 // handle error
//		 }
//	 	 // cs is populated
//	  }
func UnmarshalFromPath(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, v)
}
