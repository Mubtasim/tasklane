package api

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"slices"
	"time"

	tasklanev1 "github.com/mubtasim/tasklane/gen/tasklane/v1"
	"github.com/mubtasim/tasklane/internal/store"
)

const (
	// maxDeadPage is ListDeadJobs' default and largest page.
	maxDeadPage = 100
	// maxRedriveIDs caps RedriveJobs' ids, so a request is one batch.
	maxRedriveIDs = 1000
	// redriveBatch is how many dead jobs one "redrive all" transaction
	// takes: short transactions instead of one that holds locks on every
	// dead row (design §15.2).
	redriveBatch = 1000
)

func (s *Server) CancelJob(ctx context.Context, req *tasklanev1.CancelJobRequest) (*tasklanev1.CancelJobResponse, error) {
	j, cancelled, err := s.store.CancelJob(ctx, req.GetId())
	if err != nil {
		return nil, toStatus(s.log, "CancelJob", err)
	}
	if cancelled {
		s.metrics.Cancelled(j.Queue)
	}
	pj, err := jobFromStore(j, false)
	if err != nil {
		return nil, toStatus(s.log, "CancelJob", err)
	}
	return &tasklanev1.CancelJobResponse{Job: pj}, nil
}

// ListDeadJobs asks the store for one job more than the page holds, so it
// knows whether another page exists and the last page never carries a token.
func (s *Server) ListDeadJobs(ctx context.Context, req *tasklanev1.ListDeadJobsRequest) (*tasklanev1.ListDeadJobsResponse, error) {
	size := int(req.GetPageSize())
	if size <= 0 || size > maxDeadPage {
		size = maxDeadPage
	}
	after, err := decodePageToken(req.GetPageToken())
	if err != nil {
		return nil, err
	}
	jobs, err := s.store.ListDeadJobs(ctx, req.GetQueue(), after, size+1)
	if err != nil {
		return nil, toStatus(s.log, "ListDeadJobs", err)
	}
	out := &tasklanev1.ListDeadJobsResponse{}
	if len(jobs) > size {
		jobs = jobs[:size]
		last := jobs[size-1]
		out.NextPageToken = encodePageToken(store.DeadCursor{FinishedAt: last.FinishedAt, ID: last.ID})
	}
	for _, j := range jobs {
		pj, err := jobFromStore(j, false)
		if err != nil {
			return nil, toStatus(s.log, "ListDeadJobs", err)
		}
		out.Jobs = append(out.Jobs, pj)
	}
	return out, nil
}

// RedriveJobs with all = true runs batch after batch until one comes back
// short. It is not atomic: if it fails halfway, the committed batches stay
// redriven, and calling it again finishes the rest.
func (s *Server) RedriveJobs(ctx context.Context, req *tasklanev1.RedriveJobsRequest) (*tasklanev1.RedriveJobsResponse, error) {
	ids := req.GetIds()
	switch {
	case req.GetAll() && len(ids) > 0:
		return nil, invalid("set either ids or all, not both")
	case !req.GetAll() && len(ids) == 0:
		return nil, invalid("set ids or all")
	case len(ids) > maxRedriveIDs:
		return nil, invalid("at most %d ids, got %d", maxRedriveIDs, len(ids))
	}
	if !req.GetAll() {
		ids = slices.Compact(slices.Sorted(slices.Values(ids)))
		n, err := s.store.RedriveDead(ctx, req.GetQueue(), ids, len(ids))
		if err != nil {
			return nil, toStatus(s.log, "RedriveJobs", err)
		}
		s.metrics.Redriven(req.GetQueue(), n)
		return &tasklanev1.RedriveJobsResponse{Redriven: int64(n), Skipped: int64(len(ids) - n)}, nil
	}
	var total int64
	for {
		n, err := s.store.RedriveDead(ctx, req.GetQueue(), nil, redriveBatch)
		if err != nil {
			return nil, toStatus(s.log, "RedriveJobs", err)
		}
		s.metrics.Redriven(req.GetQueue(), n)
		total += int64(n)
		if n < redriveBatch {
			return &tasklanev1.RedriveJobsResponse{Redriven: total}, nil
		}
	}
}

// Page tokens are the last job's (finished_at in microseconds, id) as 16
// bytes, base64url-encoded. Postgres keeps timestamps in microseconds, so
// the round trip is exact. Clients must treat them as opaque.
func encodePageToken(c store.DeadCursor) string {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b, uint64(c.FinishedAt.UnixMicro()))
	binary.BigEndian.PutUint64(b[8:], uint64(c.ID))
	return base64.RawURLEncoding.EncodeToString(b)
}

var (
	minTokenMicros = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	maxTokenMicros = time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC).UnixMicro()
)

func decodePageToken(s string) (*store.DeadCursor, error) {
	if s == "" {
		return nil, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(b) != 16 {
		return nil, invalid("page_token is not a token from ListDeadJobs")
	}
	micros, id := int64(binary.BigEndian.Uint64(b)), int64(binary.BigEndian.Uint64(b[8:]))
	// A forged timestamp outside years 1-9999 would fail in the database
	// driver and surface as INTERNAL, so refuse it here.
	if micros < minTokenMicros || micros > maxTokenMicros || id < 1 {
		return nil, invalid("page_token is not a token from ListDeadJobs")
	}
	return &store.DeadCursor{FinishedAt: time.UnixMicro(micros), ID: id}, nil
}
