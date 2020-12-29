package sqlite

import (
	"context"
	"errors"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-flags"
	"github.com/whosonfirst/go-whosonfirst-flags/existential"
	wof_geojson "github.com/whosonfirst/go-whosonfirst-geojson-v2"
	"github.com/whosonfirst/go-whosonfirst-search/filter"
	"github.com/whosonfirst/go-whosonfirst-search/fulltext"
	"github.com/whosonfirst/go-whosonfirst-spr"
	wof_sqlite "github.com/whosonfirst/go-whosonfirst-sqlite"
	"github.com/whosonfirst/go-whosonfirst-sqlite-features/tables"
	wof_database "github.com/whosonfirst/go-whosonfirst-sqlite/database"
	"github.com/whosonfirst/go-whosonfirst-uri"
	_ "log"
	"net/url"
	"sync"
)

type SQLiteFullTextDatabase struct {
	fulltext.FullTextDatabase
	db           *wof_database.SQLiteDatabase
	spr_table    wof_sqlite.Table
	search_table wof_sqlite.Table
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

	sqlite_db, err := wof_database.NewDB(dsn)

	if err != nil {
		return nil, err
	}

	search_table, err := tables.NewSearchTableWithDatabase(sqlite_db)

	if err != nil {
		return nil, err
	}

	spr_table, err := tables.NewSPRTableWithDatabase(sqlite_db)

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

func (ftdb *SQLiteFullTextDatabase) IndexFeature(ctx context.Context, f wof_geojson.Feature) error {

	ftdb.mu.Lock()
	defer ftdb.mu.Unlock()

	err := ftdb.search_table.IndexRecord(ftdb.db, f)

	if err != nil {
		return err
	}

	err = ftdb.spr_table.IndexRecord(ftdb.db, f)

	if err != nil {
		return err
	}

	return nil
}

func (ftdb *SQLiteFullTextDatabase) QueryString(ctx context.Context, term string, filters ...filter.Filter) (spr.StandardPlacesResults, error) {

	conn, err := ftdb.db.Conn()

	if err != nil {
		return nil, err
	}

	q := fmt.Sprintf("SELECT id FROM %s WHERE names_all MATCH ?", ftdb.search_table.Name())

	rows, err := conn.QueryContext(ctx, q, term)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	done_ch := make(chan bool)
	err_ch := make(chan error)
	spr_ch := make(chan spr.StandardPlacesResult)

	spr_results := make([]spr.StandardPlacesResult, 0)

	remaining := 0

	for rows.Next() {

		var id int64

		err := rows.Scan(&id)

		if err != nil {
			return nil, err
		}

		remaining += 1

		go func(id int64) {

			defer func() {
				done_ch <- true
			}()

			select {
			case <-ctx.Done():
				return
			default:
				// pass
			}

			spr_r, err := ftdb.retrieveSPR(ctx, id, "")

			if err != nil {
				err_ch <- err
				return
			}

			spr_ch <- spr_r

		}(id)
	}

	for remaining > 0 {
		select {
		case <-done_ch:
			remaining -= 1
		case err := <-err_ch:
			return nil, err
		case spr_r := <-spr_ch:
			spr_results = append(spr_results, spr_r)
		}
	}

	r := &SQLiteResults{
		Places: spr_results,
	}

	return r, nil
}

// this should be moved in to go-whosonfirst-sqlite-features

func (ftdb *SQLiteFullTextDatabase) retrieveSPR(ctx context.Context, id int64, alt_label string) (spr.StandardPlacesResult, error) {

	conn, err := ftdb.db.Conn()

	if err != nil {
		return nil, err
	}

	args := []interface{}{
		id,
		alt_label,
	}

	// supersedes and superseding need to be added here pending
	// https://github.com/whosonfirst/go-whosonfirst-sqlite-features/issues/14

	spr_q := fmt.Sprintf(`SELECT 
		id, parent_id, name, placetype,
		country, repo,
		latitude, longitude,
		min_latitude, min_longitude,
		max_latitude, max_longitude,
		is_current, is_deprecated, is_ceased,
		is_superseded, is_superseding,
		lastmodified
	FROM %s WHERE id = ? AND alt_label = ?`, ftdb.spr_table.Name())

	row := conn.QueryRowContext(ctx, spr_q, args...)

	var spr_id string
	var parent_id string
	var name string
	var placetype string
	var country string
	var repo string

	var latitude float64
	var longitude float64
	var min_latitude float64
	var max_latitude float64
	var min_longitude float64
	var max_longitude float64

	var is_current int64
	var is_deprecated int64
	var is_ceased int64
	var is_superseded int64
	var is_superseding int64

	// supersedes and superseding need to be added here pending
	// https://github.com/whosonfirst/go-whosonfirst-sqlite-features/issues/14

	var lastmodified int64

	// supersedes and superseding need to be added here pending
	// https://github.com/whosonfirst/go-whosonfirst-sqlite-features/issues/14

	err = row.Scan(
		&spr_id, &parent_id, &name, &placetype, &country, &repo,
		&latitude, &longitude, &min_latitude, &max_latitude, &min_longitude, &max_longitude,
		&is_current, &is_deprecated, &is_ceased, &is_superseded, &is_superseding,
		&lastmodified,
	)

	if err != nil {
		return nil, err
	}

	path, err := uri.Id2RelPath(id)

	if err != nil {
		return nil, err
	}

	s := &SQLiteStandardPlacesResult{
		WOFId:           spr_id,
		WOFParentId:     parent_id,
		WOFName:         name,
		WOFCountry:      country,
		WOFPlacetype:    placetype,
		MZLatitude:      latitude,
		MZLongitude:     longitude,
		MZMinLatitude:   min_latitude,
		MZMaxLatitude:   max_latitude,
		MZMinLongitude:  min_longitude,
		MZMaxLongitude:  max_longitude,
		MZIsCurrent:     is_current,
		MZIsDeprecated:  is_deprecated,
		MZIsCeased:      is_ceased,
		MZIsSuperseded:  is_superseded,
		MZIsSuperseding: is_superseding,
		// supersedes and superseding go here pending
		// https://github.com/whosonfirst/go-whosonfirst-sqlite-features/issues/14
		WOFPath:         path,
		WOFRepo:         repo,
		WOFLastModified: lastmodified,
	}

	return s, nil
}

type SQLiteStandardPlacesResult struct {
	spr.StandardPlacesResult `json:",omitempty"`
	WOFId                    string  `json:"wof:id"`
	WOFParentId              string  `json:"wof:parent_id"`
	WOFName                  string  `json:"wof:name"`
	WOFCountry               string  `json:"wof:country"`
	WOFPlacetype             string  `json:"wof:placetype"`
	MZLatitude               float64 `json:"mz:latitude"`
	MZLongitude              float64 `json:"mz:longitude"`
	MZMinLatitude            float64 `json:"mz:min_latitude"`
	MZMinLongitude           float64 `json:"mz:min_longitude"`
	MZMaxLatitude            float64 `json:"mz:max_latitude"`
	MZMaxLongitude           float64 `json:"mz:max_longitude"`
	MZIsCurrent              int64   `json:"mz:is_current"`
	MZIsDeprecated           int64   `json:"mz:is_deprecated"`
	MZIsCeased               int64   `json:"mz:is_ceased"`
	MZIsSuperseded           int64   `json:"mz:is_superseded"`
	MZIsSuperseding          int64   `json:"mz:is_superseding"`

	// supersedes and superseding need to be added here pending
	// https://github.com/whosonfirst/go-whosonfirst-sqlite-features/issues/14

	WOFPath         string `json:"wof:path"`
	WOFRepo         string `json:"wof:repo"`
	WOFLastModified int64  `json:"wof:lastmodified"`
}

func (spr *SQLiteStandardPlacesResult) Id() string {
	return spr.WOFId
}

func (spr *SQLiteStandardPlacesResult) ParentId() string {
	return spr.WOFParentId
}

func (spr *SQLiteStandardPlacesResult) Name() string {
	return spr.WOFName
}

func (spr *SQLiteStandardPlacesResult) Placetype() string {
	return spr.WOFPlacetype
}

func (spr *SQLiteStandardPlacesResult) Country() string {
	return spr.WOFCountry
}

func (spr *SQLiteStandardPlacesResult) Repo() string {
	return spr.WOFRepo
}

func (spr *SQLiteStandardPlacesResult) Path() string {
	return spr.WOFPath
}

func (spr *SQLiteStandardPlacesResult) URI() string {
	return ""
}

func (spr *SQLiteStandardPlacesResult) Latitude() float64 {
	return spr.MZLatitude
}

func (spr *SQLiteStandardPlacesResult) Longitude() float64 {
	return spr.MZLongitude
}

func (spr *SQLiteStandardPlacesResult) MinLatitude() float64 {
	return spr.MZMinLatitude
}

func (spr *SQLiteStandardPlacesResult) MinLongitude() float64 {
	return spr.MZMinLongitude
}

func (spr *SQLiteStandardPlacesResult) MaxLatitude() float64 {
	return spr.MZMaxLatitude
}

func (spr *SQLiteStandardPlacesResult) MaxLongitude() float64 {
	return spr.MZMaxLongitude
}

func (spr *SQLiteStandardPlacesResult) IsCurrent() flags.ExistentialFlag {
	return existentialFlag(spr.MZIsCurrent)
}

func (spr *SQLiteStandardPlacesResult) IsCeased() flags.ExistentialFlag {
	return existentialFlag(spr.MZIsCeased)
}

func (spr *SQLiteStandardPlacesResult) IsDeprecated() flags.ExistentialFlag {
	return existentialFlag(spr.MZIsDeprecated)
}

func (spr *SQLiteStandardPlacesResult) IsSuperseded() flags.ExistentialFlag {
	return existentialFlag(spr.MZIsSuperseded)
}

func (spr *SQLiteStandardPlacesResult) IsSuperseding() flags.ExistentialFlag {
	return existentialFlag(spr.MZIsSuperseding)
}

// https://github.com/whosonfirst/go-whosonfirst-sqlite-features/issues/14

func (spr *SQLiteStandardPlacesResult) SupersededBy() []int64 {
	return []int64{}
}

// https://github.com/whosonfirst/go-whosonfirst-sqlite-features/issues/14

func (spr *SQLiteStandardPlacesResult) Supersedes() []int64 {
	return []int64{}
}

func (spr *SQLiteStandardPlacesResult) LastModified() int64 {
	return spr.WOFLastModified
}

func existentialFlag(i int64) flags.ExistentialFlag {
	fl, _ := existential.NewKnownUnknownFlag(i)
	return fl
}

type SQLiteResults struct {
	spr.StandardPlacesResults `json:",omitempty"`
	Places                    []spr.StandardPlacesResult `json:"places"`
}

func (r *SQLiteResults) Results() []spr.StandardPlacesResult {
	return r.Places
}
