package api

// Request and response limits (design §6, Limits). They keep every response
// within the gRPC message size both ends are configured for, and turn values
// the database can't store into INVALID_ARGUMENT instead of INTERNAL.
const (
	// MaxPayloadBytes caps one job's payload twice: as encoded JSON (what the
	// database stores) and as a protobuf Struct (what Lease sends back). The
	// Struct is the larger of the two, up to about 6x for number-heavy JSON.
	MaxPayloadBytes = 256 << 10
	// MaxMessageBytes is the gRPC send and receive limit on the server and
	// on the gateway's client connection, and the HTTP request body limit.
	MaxMessageBytes = 16 << 20
	// MaxLeaseJobs caps one Lease: larger max_jobs values are clamped to it.
	// Because MaxPayloadBytes bounds the protobuf size, MaxLeaseJobs jobs,
	// each at most MaxPayloadBytes plus up to 16 KiB of other fields
	// (last_error alone can be 4 KiB), fit in MaxMessageBytes (checked by
	// TestFullLeaseFitsInOneMessage): 50 × 272 KiB ≈ 13.3 MiB.
	MaxLeaseJobs = 50

	// maxBatchSlack is kept free below MaxMessageBytes in an EnqueueBatch.
	maxBatchSlack = 64 << 10

	maxKindBytes           = 255
	maxIdempotencyKeyBytes = 255
	maxTraceContextBytes   = 512
)
