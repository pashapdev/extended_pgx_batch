package extended_pgx_batch //nolint:stylecheck

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Batch struct {
	*pgx.Batch
}

func New() *Batch {
	return &Batch{
		&pgx.Batch{},
	}
}

func (b *Batch) Exec(ctx context.Context, db *pgxpool.Pool) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}

	batchResults := tx.SendBatch(ctx, b.Batch)
	if _, err = batchResults.Exec(); err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			return wrapRollbackError(rollbackErr)
		}
		return fmt.Errorf("failed to exec batch query: %w", err)
	}
	err = batchResults.Close()
	if err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			return wrapRollbackError(rollbackErr)
		}
		return fmt.Errorf("failed to close batch result: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			return wrapRollbackError(rollbackErr)
		}
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func wrapRollbackError(rollbackErr error) error {
	return fmt.Errorf("failed to rollback transaction: %w", rollbackErr)
}
