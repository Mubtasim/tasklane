package tasklane

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

// payloadToStruct JSON-encodes v into the protobuf Struct the API carries.
// nil, or a value that encodes to null (a nil map or pointer), means no
// payload. Anything that isn't a JSON object is an error.
func payloadToStruct(v any) (*structpb.Struct, error) {
	if v == nil {
		return nil, nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("tasklane: encode payload: %w", err)
	}
	if string(raw) == "null" {
		return nil, nil
	}
	s := &structpb.Struct{}
	if err := protojson.Unmarshal(raw, s); err != nil {
		return nil, fmt.Errorf("tasklane: payload must encode to a JSON object: %w", err)
	}
	return s, nil
}

// payloadFromStruct turns a leased job's payload back into JSON; no payload
// is the empty object.
func payloadFromStruct(s *structpb.Struct) (json.RawMessage, error) {
	if s == nil {
		return json.RawMessage("{}"), nil
	}
	return protojson.Marshal(s)
}
