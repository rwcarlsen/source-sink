package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math"
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
		fatal(ctx.WalkAll())
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

var (
	preExecStmts = []string{
		"CREATE TABLE IF NOT EXISTS Inventories (SimID TEXT,ResID INTEGER,AgentID INTEGER,StartTime INTEGER,EndTime INTEGER);",
		"CREATE INDEX IF NOT EXISTS res_id ON Resources(SimID ASC,ID ASC);",
		"CREATE INDEX IF NOT EXISTS res_par1 ON Resources(Parent1 ASC);",
		"CREATE INDEX IF NOT EXISTS res_par2 ON Resources(Parent2 ASC);",
		"CREATE INDEX IF NOT EXISTS res_state ON Resources(StateID ASC);",
		"CREATE INDEX IF NOT EXISTS comp_id ON Compositions(ID ASC);",
		"CREATE INDEX IF NOT EXISTS comp_iso ON Compositions(IsoID ASC);",
		"CREATE INDEX IF NOT EXISTS trans_id ON Transactions(ID ASC);",
		"CREATE INDEX IF NOT EXISTS trans_time ON Transactions(Time ASC);",
		"CREATE INDEX IF NOT EXISTS trans_receiver ON Transactions(ReceiverID ASC);",
		"CREATE INDEX IF NOT EXISTS transres_transid ON TransactedResources(TransactionID ASC);",
		"CREATE INDEX IF NOT EXISTS transres_resid ON TransactedResources(ResourceID ASC);",
		"CREATE INDEX IF NOT EXISTS rescreate_resid ON ResCreators(SimID ASC,ResID ASC);",
		"CREATE INDEX IF NOT EXISTS agent_proto ON Agents(Prototype ASC);",
		"CREATE INDEX IF NOT EXISTS agent_id ON Agents(ID ASC);",
		// simid indexes
		//"CREATE INDEX IF NOT EXISTS res_simid ON Resources(SimID ASC,Parent1 ASC,Parent2 ASC);",
		//"CREATE INDEX IF NOT EXISTS trans_simid ON Transactions(SimID ASC,ID ASC);",
		//"CREATE INDEX IF NOT EXISTS transres_simid ON TransactedResources(SimID ASC,TransactionID ASC,ResourceID ASC);",

		"CREATE INDEX IF NOT EXISTS simid_res ON Resources(SimID ASC);",
		//"CREATE INDEX IF NOT EXISTS simid_transres ON TransactedResources(SimID ASC);",
		//"CREATE INDEX IF NOT EXISTS simid_comp ON Compositions(SimID ASC);",
		//"CREATE INDEX IF NOT EXISTS simid_trans ON Transactions(SimID ASC);",
		//"CREATE INDEX IF NOT EXISTS simid_rescreate ON ResCreators(SimID ASC);",
		//"CREATE INDEX IF NOT EXISTS simid_agent ON Agents(SimID ASC);",
	}
	postExecStmts = []string{
		"CREATE INDEX IF NOT EXISTS inv_agent ON Inventories(SimID ASC,AgentID ASC);",
		"CREATE INDEX IF NOT EXISTS inv_start ON Inventories(SimID ASC,StartTime ASC);",
		"CREATE INDEX IF NOT EXISTS inv_end ON Inventories(SimID ASC,EndTime ASC);",
	}
	dumpSql = "INSERT INTO Inventories VALUES (?,?,?,?,?);"
	resSqlHead  = "SELECT ID,TimeCreated FROM "
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
			return err
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

type TmpResRow struct {
	Id int
	Created int
	Parent1 int
	Parent2 int
}

type Context struct {
	*sqlite3.Conn
	Simid     string
	tmpResTbl string
	tmpResStmt *sqlite3.Stmt
	dumpStmt  *sqlite3.Stmt
	ownerStmt *sqlite3.Stmt
	resCount  int
	Nodes     []*Node
}

func (c *Context) init() (err error) {
	c.Nodes = make([]*Node, 0, 10000)

	// create temp res table without simid
	fmt.Println("Creating temporary resource table")
	c.tmpResTbl = "tmp-restbl-" + c.Simid
	err = c.Exec("CREATE TABLE " + c.tmpResTbl + " (ID INTEGER, TimeCreated INTEGER, Parent1 INTEGER, Parent2 INTEGER)")
	if err != nil {
		return err
	}
	insert := "INSERT INTO " + c.tmpResTbl + " VALUES(?,?,?,?);"
	stmt, err := c.Query("SELECT ID,TimeCreated,Parent1,Parent2 FROM Resources WHERE SimID = ?", c.Simid)
	rows := []*TmpResRow{}
	for ; err == nil; err = stmt.Next() {
        row := &TmpResRow{}
		err := stmt.Scan(&row.Id, &row.Created, &row.Parent1, &row.Parent2)
		if err != nil {
			return err
		}
		rows = append(rows, row)
		if len(rows) >= dumpfreq {
			for _, r := range rows {
				err := c.Exec(insert, r.Id, r.Created, r.Parent1, r.Parent2)
				if err != nil {
					return err
				}
			}
			rows = rows[:0]
		}
	}
	if err != io.EOF {
		return err
	}

	// create prepared statements
	c.tmpResStmt, err = c.Prepare(resSqlHead + c.tmpResTbl + resSqlTail)
	if err != nil {
		return err
	}

	c.dumpStmt, err = c.Prepare(dumpSql)
	if err != nil {
		return err
	}
	c.ownerStmt, err = c.Prepare(ownerSql)
	if err != nil {
		return err
	}
	return nil
}

func (c *Context) WalkAll() (err error) {
	if err := c.init(); err != nil {
		return err
	}
	fmt.Println("Retrieving root resource nodes...")
	roots, err := c.getRoots()
	if err != nil {
		return err
	}
	fmt.Printf("Found %v root nodes\n", len(roots))
	for i, n := range roots {
		fmt.Printf("Processing root %d...\n", i)
		if err := c.walkNode(n); err != nil {
			return err
		}
	}
	c.Exec("DROP TABLE " + c.tmpResTbl)
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
		return err
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
			return err
		}
	}

	ti.Start("res-loop")
	// find resource's children and resource owners
	kids := make([]*Node, 0, 2)

	ti.Start("res-query")
	err = c.tmpResStmt.Query(node.ResId, node.ResId)
	ti.Stop("res-query")
	//fmt.Printf("res-query: %v\n", ti.Totals["res-query"])

	for ; err == nil; err = c.tmpResStmt.Next() {
		//fmt.Println("node: ",node)
		ti.Start("res-inner")
		ti.Start("res-scan")
		child := &Node{EndTime: math.MaxInt32}
		if err := c.tmpResStmt.Scan(&child.ResId, &child.StartTime); err != nil {
			return err
		}
		ti.Stop("res-scan")
		//fmt.Printf("res-scan: %v\n", ti.Totals["res-scan"])

		ti.Start("res-owners")
		owners, times, err := c.getNewOwners(node.ResId)
		if err != nil {
			return err
		}
		ti.Stop("res-owners")
		//fmt.Printf("res-owners: %v\n", ti.Totals["res-owners"])

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
		ti.Stop("res-inner")
		//fmt.Printf("res-inner: %v\n", ti.Totals["res-inner"])
	}
	ti.Stop("res-loop")
	fmt.Printf("res-loop: %v\n", ti.Totals["res-loop"])
	if err != io.EOF {
		return err
	}

	// walk down resource's children
	for _, child := range kids {
		err := c.walkDown(child)
		if err != nil {
			return err
		}
	}

	c.Nodes = append(c.Nodes, node)
	return nil
}

var ti = NewTimer()

func (c *Context) getNewOwners(id int) (owners, times []int, err error) {
	var owner, t int
	ti.Start("owner-loop")

	ti.Start("owner-query")
	err = c.ownerStmt.Query(id, c.Simid, c.Simid)
	ti.Stop("owner-query")
	//fmt.Printf("owner-query: %v\n", ti.Totals["owner-query"])
	for ; err == nil; err = c.ownerStmt.Next() {
		ti.Start("owner-scan")
		if err := c.ownerStmt.Scan(&owner, &t); err != nil {
			return nil, nil, err
		}
		ti.Stop("owner-scan")
		//fmt.Printf("owner-scan: %v\n", ti.Totals["owner-scan"])
		owners = append(owners, owner)
		times = append(times, t)
	}
	ti.Stop("owner-loop")
	fmt.Printf("owner-loop: %v\n", ti.Totals["owner-loop"])
	if err != io.EOF {
		return nil, nil, err
	}
	return owners, times, nil
}

func (c *Context) dumpNodes() (err error) {
	fmt.Printf("dumping inventories (%d resources done)\n", c.resCount)
	if err := c.Exec("BEGIN TRANSACTION;"); err != nil {
		return err
	}
	for _, n := range c.Nodes {
		if err = c.dumpStmt.Exec(c.Simid, n.ResId, n.OwnerId, n.StartTime, n.EndTime); err != nil {
			return err
		}
	}
	if err := c.Exec("END TRANSACTION;"); err != nil {
		return err
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
