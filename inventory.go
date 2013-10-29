package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime/pprof"

	"code.google.com/p/go-sqlite/go1/sqlite3"
)

type Node struct {
	ResId     int
	OwnerId   int
	StartTime int
	EndTime   int
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

const dumpfreq = 100000

var mappednodes = map[int32]struct{}{}

func main() {
	log.SetFlags(0)
	flag.Parse()
	fname := flag.Arg(0)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	conn, err := sqlite3.Open(fname)
	fatal(err)
	ctx := &Context{Conn: conn}
	defer ctx.Close()
	fatal(ctx.Init())

	fmt.Println("Retrieving root resource nodes...")
	roots, err := GetRoots(conn)
	fatal(err)
	fmt.Printf("Found %v root nodes\n", len(roots))

	for i, root := range roots {
		fmt.Printf("Processing root %d...\n", i)
		err := ctx.WalkDown(root)
		fatal(err)
	}
}

func GetRoots(conn *sqlite3.Conn) (roots []*Node, err error) {
	stmt, err := conn.Query("SELECT COUNT(*) FROM ResCreators")
	if err != nil {
		return nil, err
	}
	n := 0
	if err := stmt.Scan(&n); err != nil {
		return nil, err
	}

	roots = make([]*Node, 0, n)
	sql := `SELECT ID,TimeCreated,ModelID FROM Resources 
	          INNER JOIN ResCreators ON ID = ResID`
	for stmt, err = conn.Query(sql); err == nil; err = stmt.Next() {
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

const (
	dumpSql  = "INSERT INTO Inventories VALUES (?,?,?,?)"
	resSql   = "SELECT ID,TimeCreated FROM Resources WHERE Parent1 = ? OR Parent2 = ?"
	ownerSql = `SELECT tr.ReceiverID, tr.Time FROM Transactions AS tr
				  INNER JOIN TransactedResources AS trr ON tr.ID = trr.TransactionID
				  WHERE trr.ResourceID = ? ORDER BY tr.Time ASC;`
)

var execStmts = []string{
	"CREATE TABLE IF NOT EXISTS Inventories (ResID INTEGER,AgentID INTEGER,StartTime INTEGER,EndTime INTEGER);",
	"CREATE INDEX IF NOT EXISTS res_id ON Resources(ID ASC);",
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
	"CREATE INDEX IF NOT EXISTS rescreate_resid ON ResCreators(ResID ASC);",
	"CREATE INDEX IF NOT EXISTS agent_proto ON Agents(Prototype ASC);",
	"CREATE INDEX IF NOT EXISTS agent_id ON Agents(ID ASC);",
}

type Context struct {
	*sqlite3.Conn
	dumpStmt  *sqlite3.Stmt
	resStmt   *sqlite3.Stmt
	ownerStmt *sqlite3.Stmt
	resCount  int
	Nodes     []*Node
}

func (c *Context) Init() (err error) {
	c.Nodes = make([]*Node, 0, 10000)

	fmt.Println("Creating indexes and inventory table...")
	for _, sql := range execStmts {
		if err := c.Exec(sql); err != nil {
			return err
		}
	}

	c.dumpStmt, err = c.Prepare(dumpSql)
	if err != nil {
		return err
	}

	c.resStmt, err = c.Prepare(resSql)
	if err != nil {
		return err
	}

	c.ownerStmt, err = c.Prepare(ownerSql)
	if err != nil {
		return err
	}

	return nil
}

func (c *Context) DumpNodes() (err error) {
	fmt.Printf("dumping inventories (%d resources done)\n", c.resCount)
	if err := c.Exec("BEGIN TRANSACTION;"); err != nil {
		return err
	}
	for _, n := range c.Nodes {
		if err = c.dumpStmt.Exec(n.ResId, n.OwnerId, n.StartTime, n.EndTime); err != nil {
			return err
		}
	}
	if err := c.Exec("END TRANSACTION;"); err != nil {
		return err
	}
	c.Nodes = c.Nodes[:0]
	return nil
}

func (c *Context) WalkDown(node *Node) (err error) {
	if _, ok := mappednodes[int32(node.ResId)]; ok {
		return
	}
	mappednodes[int32(node.ResId)] = struct{}{}

	// dump if necessary
	c.resCount++
	if c.resCount%dumpfreq == 0 {
		if err := c.DumpNodes(); err != nil {
			return err
		}
	}

	// find resource's children and resource owners
	kids := make([]*Node, 0, 2)
	for err = c.resStmt.Query(node.ResId, node.ResId); err == nil; err = c.resStmt.Next() {
		child := &Node{EndTime: math.MaxInt32}
		if err := c.resStmt.Scan(&child.ResId, &child.StartTime); err != nil {
			return err
		}

		owners, times, err := c.GetNewOwners(node.ResId)
		if err != nil {
			return err
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
		return err
	}

	// walk down resource's children
	for _, child := range kids {
		err := c.WalkDown(child)
		if err != nil {
			return err
		}
	}

	c.Nodes = append(c.Nodes, node)
	return nil
}

func (c *Context) Close() (err error) {
	if err2 := c.DumpNodes(); err2 != nil {
		err = err2
	}
	if err2 := c.Conn.Close(); err2 != nil {
		err = err2
	}
	return err
}

func (c *Context) GetNewOwners(id int) (owners, times []int, err error) {
	var owner, t int
	for err = c.ownerStmt.Query(id); err == nil; err = c.ownerStmt.Next() {
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

func fatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
