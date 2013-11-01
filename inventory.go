package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"strings"
	"time"

	"code.google.com/p/go-sqlite/go1/sqlite3"
)

const dumpfreq = 100000

var mappednodes = map[int32]struct{}{}

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
		err := ctx.WalkAll()
		if err != nil {
			fmt.Println(err)
		}
		fatal(err)
	}
}

func GetSimIds(conn *sqlite3.Conn) (ids []string, err error) {
	sql := "SELECT SimID FROM SimulationTimeInfo"
	var stmt *sqlite3.Stmt
	for stmt, err = conn.Query(sql); err == nil; err = stmt.Next() {
		var s string
		if err := stmt.Scan(&s); err != nil {
			return nil, err
		}
		ids = append(ids, s)
	}
	if err != io.EOF {
		return nil, err
	}
	return ids, nil
}

func Index(table string, cols ...string) string {
	var buf bytes.Buffer
	buf.WriteString("CREATE INDEX IF NOT EXISTS ")
	buf.WriteString(table + "_" + cols[0])
	for _, c := range cols[1:] {
		buf.WriteString("_" + c)
	}
	buf.WriteString(" ON " + table + " (" + cols[0] + " ASC")
	for _, c := range cols[1:] {
		buf.WriteString("," + c + " ASC")
	}
	buf.WriteString(");")
	return buf.String()
}

var (
	preExecStmts = []string{
		"DROP TABLE IF EXISTS Inventories",
		"CREATE TABLE Inventories (SimID TEXT,ResID INTEGER,AgentID INTEGER,StartTime INTEGER,EndTime INTEGER);",
		Index("Resources", "SimID", "ID"),
		Index("Resources", "Parent1"),
		Index("Resources", "Parent2"),
		Index("Resources", "StateID"),
		Index("Compositions", "ID"),
		Index("Compositions", "IsoID"),
		Index("Transactions", "ID"),
		Index("Transactions", "Time"),
		Index("Transactions", "ReceiverID"),
		Index("TransactedResources", "TransactionID"),
		Index("TransactedResources", "ResourceID"),
		Index("ResCreators", "SimID", "ResID"),
		Index("Agents", "Prototype"),
		Index("Agents", "ID"),
	}
	postExecStmts = []string{
		Index("Inventories", "SimID", "AgentID"),
		Index("Inventories", "SimID", "StartTime"),
		Index("Inventories", "SimID", "EndTime"),
	}
	dumpSql    = "INSERT INTO Inventories VALUES (?,?,?,?,?);"
	resSqlHead = "SELECT ID,TimeCreated FROM "
	resSqlTail = " WHERE Parent1 = ? OR Parent2 = ?;"

	ownerSql = `SELECT tr.ReceiverID, tr.Time FROM Transactions AS tr
				  INNER JOIN TransactedResources AS trr ON tr.ID = trr.TransactionID
				  WHERE trr.ResourceID = ? AND tr.SimID = ? AND trr.SimID = ?
				  ORDER BY tr.Time ASC;`
	rootsSql = `SELECT res.ID,res.TimeCreated,rc.ModelID FROM Resources AS res
				  INNER JOIN ResCreators AS rc ON res.ID = rc.ResID
				  WHERE res.SimID = ? AND rc.SimID = ?;`
)

func Prepare(conn *sqlite3.Conn) (err error) {
	fmt.Println("Creating indexes and inventory table...")
	for _, sql := range preExecStmts {
		if err := conn.Exec(sql); err != nil {
			fmt.Println("    ", err)
		}
	}
	return nil
}

func Finish(conn *sqlite3.Conn) (err error) {
	fmt.Println("Creating inventory indexes...")
	for _, sql := range postExecStmts {
		if err := conn.Exec(sql); err != nil {
			return err
		}
	}
	return nil
}

type Node struct {
	ResId     int
	OwnerId   int
	StartTime int
	EndTime   int
}

type Context struct {
	*sqlite3.Conn
	Simid      string
	tmpResTbl  string
	tmpResStmt *sqlite3.Stmt
	dumpStmt   *sqlite3.Stmt
	ownerStmt  *sqlite3.Stmt
	resCount   int
	Nodes      []*Node
}

func (c *Context) init() (err error) {
	c.Nodes = make([]*Node, 0, 10000)

	// create temp res table without simid
	fmt.Println("Creating temporary resource table...")
	c.tmpResTbl = "tmp_restbl_" + strings.Replace(c.Simid, "-", "_", -1)
	if err := c.Exec("DROP TABLE IF EXISTS " + c.tmpResTbl); err != nil {
		panic(err)
	}
	sql := "CREATE TABLE " + c.tmpResTbl + " AS SELECT ID,TimeCreated,Parent1,Parent2 FROM Resources WHERE SimID = ?;"
	if err := c.Exec(sql, c.Simid); err != nil {
		panic(err)
	}
	fmt.Println("Indexing temporary resource table...")
	if err := c.Exec(Index(c.tmpResTbl, "Parent1")); err != nil {
		panic(err)
	}
	if err := c.Exec(Index(c.tmpResTbl, "Parent2")); err != nil {
		panic(err)
	}

	// create prepared statements
	c.tmpResStmt, err = c.Prepare(resSqlHead + c.tmpResTbl + resSqlTail)
	if err != nil {
		panic(err)
	}

	c.dumpStmt, err = c.Prepare(dumpSql)
	if err != nil {
		panic(err)
	}
	c.ownerStmt, err = c.Prepare(ownerSql)
	if err != nil {
		panic(err)
	}
	return nil
}

func (c *Context) WalkAll() (err error) {
	if err := c.init(); err != nil {
		panic(err)
	}
	fmt.Println("Retrieving root resource nodes...")
	roots, err := c.getRoots()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Found %v root nodes\n", len(roots))
	for i, n := range roots {
		fmt.Printf("    Processing root %d...\n", i)
		if err := c.walkNode(n); err != nil {
			panic(err)
		}
	}
	fmt.Println("Dropping temporary resource table...")
	if err := c.Exec("DROP TABLE " + c.tmpResTbl); err != nil {
		panic(err)
	}
	return c.dumpNodes()
}

func (c *Context) getRoots() (roots []*Node, err error) {
	sql := "SELECT COUNT(*) FROM ResCreators WHERE SimID = ?"
	stmt, err := c.Query(sql, c.Simid)
	if err != nil {
		return nil, err
	}
	n := 0
	if err := stmt.Scan(&n); err != nil {
		return nil, err
	}
	stmt.Reset()

	roots = make([]*Node, 0, n)
	for stmt, err = c.Query(rootsSql, c.Simid, c.Simid); err == nil; err = stmt.Next() {
		node := &Node{EndTime: math.MaxInt32}
		if err := stmt.Scan(&node.ResId, &node.StartTime, &node.OwnerId); err != nil {
			return nil, err
		}
		roots = append(roots, node)
	}
	if err != io.EOF {
		return nil, err
	}
	return roots, nil
}

func (c *Context) walkNode(node *Node) (err error) {
	if err := c.walkDown(node); err != nil {
		panic(err)
	}
	return nil
}

func (c *Context) walkDown(node *Node) (err error) {
	if _, ok := mappednodes[int32(node.ResId)]; ok {
		return
	}
	mappednodes[int32(node.ResId)] = struct{}{}

	// dump if necessary
	c.resCount++
	if c.resCount%dumpfreq == 0 {
		if err := c.dumpNodes(); err != nil {
			panic(err)
		}
	}

	// find resource's children and resource owners
	kids := make([]*Node, 0, 2)

	err = c.tmpResStmt.Query(node.ResId, node.ResId)

	for ; err == nil; err = c.tmpResStmt.Next() {
		child := &Node{EndTime: math.MaxInt32}
		if err := c.tmpResStmt.Scan(&child.ResId, &child.StartTime); err != nil {
			panic(err)
		}

		owners, times, err := c.getNewOwners(node.ResId)
		if err != nil {
			panic(err)
		}

		if len(owners) > 0 {
			node.EndTime = times[0]
			child.OwnerId = owners[len(owners)-1]

			times = append(times, child.StartTime)
			for i := range owners {
				n := &Node{ResId: node.ResId, OwnerId: owners[i], StartTime: times[i], EndTime: times[i+1]}
				c.Nodes = append(c.Nodes, n)
			}
		} else {
			node.EndTime = child.StartTime
			child.OwnerId = node.OwnerId
		}

		kids = append(kids, child)
	}
	if err != io.EOF {
		panic(err)
	}

	// walk down resource's children
	for _, child := range kids {
		err := c.walkDown(child)
		if err != nil {
			panic(err)
		}
	}

	c.Nodes = append(c.Nodes, node)
	return nil
}

var ti = NewTimer()

func (c *Context) getNewOwners(id int) (owners, times []int, err error) {
	var owner, t int
	err = c.ownerStmt.Query(id, c.Simid, c.Simid)
	for ; err == nil; err = c.ownerStmt.Next() {
		if err := c.ownerStmt.Scan(&owner, &t); err != nil {
			return nil, nil, err
		}
		owners = append(owners, owner)
		times = append(times, t)
	}
	if err != io.EOF {
		return nil, nil, err
	}
	return owners, times, nil
}

func (c *Context) dumpNodes() (err error) {
	fmt.Printf("Dumping inventories (%d resources done)...\n", c.resCount)
	if err := c.Exec("BEGIN TRANSACTION;"); err != nil {
		panic(err)
	}
	for _, n := range c.Nodes {
		if err = c.dumpStmt.Exec(c.Simid, n.ResId, n.OwnerId, n.StartTime, n.EndTime); err != nil {
			panic(err)
		}
	}
	if err := c.Exec("END TRANSACTION;"); err != nil {
		panic(err)
	}
	c.Nodes = c.Nodes[:0]
	return nil
}

func fatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

type Timer struct {
	starts map[string]time.Time
	Totals map[string]time.Duration
}

func NewTimer() *Timer {
	return &Timer{
		map[string]time.Time{},
		map[string]time.Duration{},
	}
}

func (t *Timer) Start(label string) {
	if _, ok := t.starts[label]; !ok {
		t.starts[label] = time.Now()
	}
}

func (t *Timer) Stop(label string) {
	if start, ok := t.starts[label]; ok {
		t.Totals[label] += time.Now().Sub(start)
	}
	delete(t.starts, label)
}
