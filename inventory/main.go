package main

import (
	"flag"
	"log"

	"code.google.com/p/go-sqlite/go1/sqlite3"
)

func main() {
	log.SetFlags(0)
	flag.Parse()
	fname := flag.Arg(0)

	conn, err := sqlite3.Open(fname)
	fatalif(err)
	defer conn.Close()

	fatalif(Prepare(conn))
	defer Finish(conn)

	simids, err := GetSimIds(conn)
	fatalif(err)

	for _, simid := range simids {
		ctx := NewContext(conn, simid)
		fatalif(ctx.WalkAll())
	}
}
