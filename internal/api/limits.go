package api

// Request and response limits (design §6, Limits). They keep every response
// within the gRPC message size both ends are configured for, and turn values
// the database can't store into INVALID_ARGUMENT instead of INTERNAL.
const (
	// MaxPayloadBytes caps one job's payload, measured as encoded JSON.
	MaxPayloadBytes = 256 << 10
	// MaxMessageBytes is the gRPC send and receive limit on the server and
	// on the gateway's client connection, and the HTTP request body limit.
	MaxMessageBytes = 16 << 20
	// MaxLeaseJobs caps one Lease: larger max_jobs values are clamped to it,
	// so MaxLeaseJobs payloads of MaxPayloadBytes fit in MaxMessageBytes.
	MaxLeaseJobs = 50

	maxKindBytes           = 255
	maxIdempotencyKeyBytes = 255
	maxTraceContextBytes   = 512
	// maxErrorBytes caps Nack's error message; longer ones are truncated.
	maxErrorBytes = 4096
)
