package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/whosonfirst/go-whosonfirst-search-sqlite"
	"github.com/whosonfirst/go-whosonfirst-search/fulltext"
	"log"
)

func main() {

	db_uri := flag.String("fulltext-database-uri", "null://", "...")

	flag.Parse()

	ctx := context.Background()

	db, err := fulltext.NewFullTextDatabase(ctx, *db_uri)

	if err != nil {
		log.Fatal(err)
	}

	for _, q := range flag.Args() {

		r, err := db.QueryString(ctx, q)

		if err != nil {
			log.Fatal(err)
		}

		enc_r, err := json.Marshal(r)

		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(string(enc_r))
	}
}
