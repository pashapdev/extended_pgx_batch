package extended_pgx_batch_test

import (
	"context"
	"fmt"
	"testing"

	extendedPgxBatch "github.com/pashapdev/extended_pgx_batch"

	"github.com/jackc/pgx/v4/pgxpool"
	dbCreater "github.com/pashapdev/db_creater"
	"github.com/stretchr/testify/require"
)

type testEntity struct {
	Content string
}

func connString(user, password, address, db string, port int) string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		address,
		port,
		db,
		user,
		password)
}

func insertContent(batch *extendedPgxBatch.Batch, testEntities []testEntity) {
	q := "INSERT INTO test_table(content) VALUES($1)"

	for i := range testEntities {
		batch.Queue(q, testEntities[i].Content)
	}
}

func selectContent(ctx context.Context, db *pgxpool.Pool) ([]testEntity, error) {
	rows, err := db.Query(ctx, "SELECT content FROM test_table")
	if err != nil {
		return nil, err
	}

	var testEntities []testEntity
	for rows.Next() {
		var content string
		err := rows.Scan(&content)
		if err != nil {
			return nil, err
		}
		testEntities = append(testEntities, testEntity{Content: content})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	defer rows.Close()

	return testEntities, nil
}

func TestExec(t *testing.T) {
	const (
		user     = "postgres"
		password = "postgres"
		address  = "localhost"
		port     = 5432
		db       = "db_test"
	)
	ctx := context.Background()

	creater := dbCreater.New(user, password, address, db, port)
	testDB, err := creater.CreateWithMigration("file://./migrations/")
	require.NoError(t, err)
	defer creater.Drop(testDB) //nolint:errcheck

	pool, err := pgxpool.Connect(ctx, connString(user, password, address, testDB, port))
	require.NoError(t, err)

	entities, err := selectContent(ctx, pool)
	require.NoError(t, err)
	require.Len(t, entities, 0)

	testData := []testEntity{
		{
			Content: "Content1",
		},
		{
			Content: "Content2",
		},
		{
			Content: "Content3",
		},
	}
	batch := extendedPgxBatch.New()
	insertContent(batch, testData)

	err = batch.Exec(ctx, pool)
	require.NoError(t, err)

	entities, err = selectContent(ctx, pool)
	require.NoError(t, err)
	require.ElementsMatch(t, entities, testData)
}
