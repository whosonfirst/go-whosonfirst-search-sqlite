# go-whosonfirst-search-sqlite

Go package that implements `whosonfirst/go-whosonfirst-search` fulltext search interface with a SQLite database.

## Description

`go-whosonfirst-search-sqlite` is a Go package that implements the [go-whosonfirst-search](https://github.com/whosonfirst/go-whosonfirst-search) fulltext search interface with a SQLite database, specifically a SQLite database with Who's On First records indexed in [go-whosonfirst-sqlite-features](https://github.com/whosonfirst/go-whosonfirst-sqlite-features) `search` and `spr` tables.

### Important

This is work in progress. Documentation to follow.

## Tools

### fulltext

```
$> ./bin/fulltext \
	-fulltext-database-uri 'sqlite://?dsn=/usr/local/data/canada-latest.db' \
	montreal \

| jq '.["places"][]["wof:name"]'

"Rive-Nord de Montréal"
"Quartier international de Montreal"
"Quartier Chinois"
"Golden Square Mile"
"Montreal"
"Quartier International de Montréal"
"Montreal-Ouest"
"Golden Square Mile"
"Montreal River"
"Rive-Sud de Montréal"
"La Petite-Italie"
"Montreal-Est"
"Downtown Montréal"
"Montréal-Nord"
"Montreal Lake I.R. 106B"
"Montreal West"
"Montreal-Pierre Elliott Trudeau International Airport"
"Montréal-Est"
"Montreal Lake I.R. 106"
"Montreal"
"Old Montreal"
"Centre-Ville"
"Communaute metropolitaine de Montreal"
"Vieux Montréal"
"Saint-Luc Montréal-Ouest"
```

This assumes a SQLite database with Who's On First records indexed in [go-whosonfirst-sqlite-features](https://github.com/whosonfirst/go-whosonfirst-sqlite-features) `search` and `spr` tables. These can be produced using the `wof-sqlite-index-features` tool which is part of the [go-whosonfirst-sqlite-features-index](https://github.com/whosonfirst/go-whosonfirst-sqlite-features-index) package. For example:

```
$> bin/wof-sqlite-index-features \
	-spr -search \
	-dsn /usr/local/data/canada-latest.db \
	-mode repo:// \
	/usr/local/data/whosonfirst-data-admin-ca
```
## See also

* https://github.com/whosonfirst/go-whosonfirst-search
* https://github.com/whosonfirst/go-whosonfirst-sqlite-features
* https://github.com/whosonfirst/go-whosonfirst-sqlite-features-index