package main

import (
	"flag"
	"log"

	"code.google.com/p/go-sqlite/go1/sqlite3"
)

const dumpfreq = 100000

func main() {
	log.SetFlags(0)
	flag.Parse()
	fname := flag.Arg(0)

	conn, err := sqlite3.Open(fname)
	fatal(err)
	defer conn.Close()

	fatal(Prepare(conn))
	defer Finish(conn)

	simids, err := GetSimIds(conn)
	fatal(err)

	for _, simid := range simids {
		ctx := &Context{Conn: conn, Simid: simid}
		fatal(ctx.WalkAll())
	}
}
