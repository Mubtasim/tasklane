package main

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// payload carries the job's scheduled send time as a decimal string of Unix
// nanoseconds. As a JSON number it would become a float64 in the protobuf
// Struct the server stores and returns, which keeps only ~16 digits: about
// 256 ns of error at today's timestamps, and an int64 field that silently
// rounds. A string arrives exact.
type payload struct {
	T   string `json:"t"`
	Pad string `json:"pad,omitempty"`
}

// payloadOverhead is the JSON around the padding: {"t":"<19 digits>","pad":""}.
const payloadOverhead = len(`{"t":"1762400000123456789","pad":""}`)

func newPayload(at time.Time, size int) payload {
	return payload{T: strconv.FormatInt(at.UnixNano(), 10), Pad: strings.Repeat("x", max(size-payloadOverhead, 0))}
}

// scheduledTime reads the time newPayload wrote.
func scheduledTime(raw json.RawMessage) (time.Time, bool) {
	var p struct {
		T string `json:"t"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return time.Time{}, false
	}
	ns, err := strconv.ParseInt(p.T, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(0, ns), true
}
