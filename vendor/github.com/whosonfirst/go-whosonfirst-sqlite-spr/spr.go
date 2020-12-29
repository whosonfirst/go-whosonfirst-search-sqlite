package spr

import (
	"context"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-flags"
	"github.com/whosonfirst/go-whosonfirst-flags/existential"
	wof_spr "github.com/whosonfirst/go-whosonfirst-spr"
	wof_sqlite "github.com/whosonfirst/go-whosonfirst-sqlite"
	wof_database "github.com/whosonfirst/go-whosonfirst-sqlite/database"
	"github.com/whosonfirst/go-whosonfirst-uri"
)

type SQLiteResults struct {
	wof_spr.StandardPlacesResults `json:",omitempty"`
	Places                        []wof_spr.StandardPlacesResult `json:"places"`
}

func (r *SQLiteResults) Results() []wof_spr.StandardPlacesResult {
	return r.Places
}

type SQLiteStandardPlacesResult struct {
	wof_spr.StandardPlacesResult `json:",omitempty"`
	WOFId                        string  `json:"wof:id"`
	WOFParentId                  string  `json:"wof:parent_id"`
	WOFName                      string  `json:"wof:name"`
	WOFCountry                   string  `json:"wof:country"`
	WOFPlacetype                 string  `json:"wof:placetype"`
	MZLatitude                   float64 `json:"mz:latitude"`
	MZLongitude                  float64 `json:"mz:longitude"`
	MZMinLatitude                float64 `json:"mz:min_latitude"`
	MZMinLongitude               float64 `json:"mz:min_longitude"`
	MZMaxLatitude                float64 `json:"mz:max_latitude"`
	MZMaxLongitude               float64 `json:"mz:max_longitude"`
	MZIsCurrent                  int64   `json:"mz:is_current"`
	MZIsDeprecated               int64   `json:"mz:is_deprecated"`
	MZIsCeased                   int64   `json:"mz:is_ceased"`
	MZIsSuperseded               int64   `json:"mz:is_superseded"`
	MZIsSuperseding              int64   `json:"mz:is_superseding"`

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

func RetrieveSPR(ctx context.Context, spr_db *wof_database.SQLiteDatabase, spr_table wof_sqlite.Table, id int64, alt_label string) (wof_spr.StandardPlacesResult, error) {

	conn, err := spr_db.Conn()

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
	FROM %s WHERE id = ? AND alt_label = ?`, spr_table.Name())

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

func existentialFlag(i int64) flags.ExistentialFlag {
	fl, _ := existential.NewKnownUnknownFlag(i)
	return fl
}
