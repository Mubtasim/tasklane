package pgstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/mubtasim/tasklane/internal/store"
)

// CancelJob is one conditional update, so it is safe against a concurrent
// Lease: if Lease locked the row first, this UPDATE waits, then re-checks
// state = 'available' on the new row version (EvalPlanQual), finds 'leased'
// and changes nothing; if Cancel locked it first, Lease skips the row.
func (s *Store) CancelJob(ctx context.Context, id int64) (store.Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx,
		`UPDATE jobs j SET state = 'cancelled', finished_at = now()
		 WHERE j.id = $1 AND j.state = 'available'
		 RETURNING `+jobColumns, id))
	if !errors.Is(err, pgx.ErrNoRows) {
		return j, err
	}
	// Nothing matched: say why. GetJob returns ErrNotFound for a missing job.
	j, err = s.GetJob(ctx, id)
	if err != nil {
		return store.Job{}, err
	}
	if j.State == store.StateCancelled {
		return j, nil
	}
	return store.Job{}, fmt.Errorf("%w: job %d is %s; only available jobs can be cancelled", store.ErrWrongState, id, j.State)
}
