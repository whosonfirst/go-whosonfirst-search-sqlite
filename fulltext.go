package sqlite

import (
	"context"
	"errors"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-search/filter"
	"github.com/whosonfirst/go-whosonfirst-search/fulltext"
	wof_spr "github.com/whosonfirst/go-whosonfirst-spr/v2"
	aa_sqlite "github.com/aaronland/go-sqlite"
	"github.com/whosonfirst/go-whosonfirst-sqlite-features/tables"
	"github.com/whosonfirst/go-whosonfirst-sqlite-spr"
	aa_database "github.com/aaronland/go-sqlite/database"
	_ "log"
	"net/url"
	"sort"
	"sync"
)

type SQLiteFullTextDatabase struct {
	fulltext.FullTextDatabase
	db           *aa_database.SQLiteDatabase
	spr_table    aa_sqlite.Table
	search_table aa_sqlite.Table
	mu           *sync.RWMutex
}

func init() {
	ctx := context.Background()
	fulltext.RegisterFullTextDatabase(ctx, "sqlite", NewSQLiteFullTextDatabase)
}

func NewSQLiteFullTextDatabase(ctx context.Context, str_uri string) (fulltext.FullTextDatabase, error) {

	u, err := url.Parse(str_uri)

	if err != nil {
		return nil, err
	}

	q := u.Query()

	dsn := q.Get("dsn")

	if dsn == "" {
		return nil, errors.New("Missing 'dsn' parameter")
	}

	sqlite_db, err := aa_database.NewDB(ctx, dsn)

	if err != nil {
		return nil, err
	}

	search_table, err := tables.NewSearchTableWithDatabase(ctx, sqlite_db)

	if err != nil {
		return nil, err
	}

	spr_table, err := tables.NewSPRTableWithDatabase(ctx, sqlite_db)

	if err != nil {
		return nil, err
	}

	mu := new(sync.RWMutex)

	ftdb := &SQLiteFullTextDatabase{
		db:           sqlite_db,
		search_table: search_table,
		spr_table:    spr_table,
		mu:           mu,
	}

	return ftdb, nil
}

func (ftdb *SQLiteFullTextDatabase) Close(ctx context.Context) error {
	return ftdb.db.Close()
}

func (ftdb *SQLiteFullTextDatabase) IndexFeature(ctx context.Context, f []byte) error {

	ftdb.mu.Lock()
	defer ftdb.mu.Unlock()

	err := ftdb.search_table.IndexRecord(ctx, ftdb.db, f)

	if err != nil {
		return err
	}

	err = ftdb.spr_table.IndexRecord(ctx, ftdb.db, f)

	if err != nil {
		return err
	}

	return nil
}

func (ftdb *SQLiteFullTextDatabase) QueryString(ctx context.Context, term string, filters ...filter.Filter) (wof_spr.StandardPlacesResults, error) {

	conn, err := ftdb.db.Conn()

	if err != nil {
		return nil, err
	}

	q := fmt.Sprintf("SELECT id FROM %s WHERE names_all MATCH ? OR id MATCH ?", ftdb.search_table.Name())

	rows, err := conn.QueryContext(ctx, q, term, term)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type SPRResult struct {
		Index int
		SPR   wof_spr.StandardPlacesResult
	}

	done_ch := make(chan bool)
	err_ch := make(chan error)
	spr_ch := make(chan SPRResult)

	spr_results := make(map[int]wof_spr.StandardPlacesResult)

	remaining := 0
	idx := 0

	for rows.Next() {

		var id int64

		err := rows.Scan(&id)

		if err != nil {
			return nil, err
		}

		remaining += 1

		go func(idx int, id int64) {

			defer func() {
				done_ch <- true
			}()

			select {
			case <-ctx.Done():
				return
			default:
				// pass
			}

			spr_r, err := spr.RetrieveSPR(ctx, ftdb.db, ftdb.spr_table, id, "")

			if err != nil {
				err_ch <- err
				return
			}

			spr_ch <- SPRResult{
				Index: idx,
				SPR:   spr_r,
			}

		}(idx, id)

		idx += 1
	}

	for remaining > 0 {
		select {
		case <-done_ch:
			remaining -= 1
		case err := <-err_ch:
			return nil, err
		case spr_r := <-spr_ch:
			spr_results[spr_r.Index] = spr_r.SPR
		}
	}

	indices := make([]int, 0)

	for i, _ := range spr_results {
		indices = append(indices, i)
	}

	sort.Ints(indices)

	sorted := make([]wof_spr.StandardPlacesResult, len(indices))

	for idx, i := range indices {
		sorted[idx] = spr_results[i]
	}

	r := &spr.SQLiteResults{
		Places: sorted,
	}

	return r, nil
}
