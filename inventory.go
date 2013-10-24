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
	defer conn.Close()

	fmt.Println("Creating indexes...")
	fatal(CreateIndex(conn))
	fmt.Println("Creating inventories table...")
	fatal(CreateNodeTable(conn))

	fmt.Println("Retrieving root resource nodes...")
	roots, err := GetRoots(conn)
	fatal(err)
	fmt.Printf("Found %v root nodes\n", len(roots))

	nodes := make([]*Node, 0, 100000)
	for i, root := range roots {
		fmt.Printf("Processing root %d...\n", i)
		err := WalkDown(conn, root, &nodes)
		fatal(err)
	}
	fmt.Println("dumping final remaining inventories...")
	fatal(DumpNodes(conn, nodes))
}

func CreateNodeTable(conn *sqlite3.Conn) (err error) {
	sql := `CREATE TABLE IF NOT EXISTS Inventories
	          (ResID INTEGER,AgentID INTEGER,StartTime INTEGER,EndTime INTEGER);`
	return conn.Exec(sql)
}

var dumpStmt *sqlite3.Stmt

func DumpNodes(conn *sqlite3.Conn, nodes []*Node) (err error) {
	if dumpStmt == nil {
		dumpStmt, err = conn.Prepare("INSERT INTO Inventories VALUES (?,?,?,?)")
		if err != nil {
			return err
		}
	}

	if err := conn.Exec("BEGIN TRANSACTION;"); err != nil {
		return err
	}
	for _, n := range nodes {
		if err = dumpStmt.Exec(n.ResId, n.OwnerId, n.StartTime, n.EndTime); err != nil {
			return err
		}
	}
	if err := conn.Exec("END TRANSACTION;"); err != nil {
		return err
	}
	return nil
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

var mappednodes = map[int32]struct{}{}
var resStmt *sqlite3.Stmt
var resCount = 0

func WalkDown(conn *sqlite3.Conn, node *Node, nodes *[]*Node) (err error) {
	if _, ok := mappednodes[int32(node.ResId)]; ok {
		return
	}
	mappednodes[int32(node.ResId)] = struct{}{}

	// dump if necessary
	resCount++
	if resCount % dumpfreq == 0 {
		fmt.Printf("dumping inventories (%d resources done)\n", resCount)
		if err := DumpNodes(conn, *nodes); err != nil {
			return err
		}
		*nodes = (*nodes)[:0]
	}

	// prime sql prepared stmt
	if resStmt == nil {
		sql := "SELECT ID,TimeCreated FROM Resources WHERE Parent1 = ? OR Parent2 = ?"
		resStmt, err = conn.Prepare(sql)
		if err != nil {
			return err
		}
	}

	// find resource's children and resource owners
	kids := make([]*Node, 0, 2)
	for err = resStmt.Query(node.ResId, node.ResId); err == nil; err = resStmt.Next() {
		child := &Node{EndTime: math.MaxInt32}
		if err := resStmt.Scan(&child.ResId, &child.StartTime); err != nil {
			return err
		}

		owners, times, err := GetNewOwners(conn, node.ResId)
		if err != nil {
			return err
		}
		if len(owners) > 0 {
			node.EndTime = times[0]
			child.OwnerId = owners[len(owners)-1]

			times = append(times, child.StartTime)
			for i := range owners {
				n := &Node{ResId: node.ResId, OwnerId: owners[i], StartTime: times[i], EndTime: times[i+1]}
				*nodes = append(*nodes, n)
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
		err := WalkDown(conn, child, nodes)
		if err != nil {
			return err
		}
	}

	*nodes = append(*nodes, node)
	return nil
}

var ownerStmt *sqlite3.Stmt

func GetNewOwners(conn *sqlite3.Conn, id int) (owners, times []int, err error) {
	sql := `SELECT tr.ReceiverID, tr.Time FROM Transactions AS tr
			  INNER JOIN TransactedResources AS trr ON tr.ID = trr.TransactionID
              WHERE trr.ResourceID = ? ORDER BY tr.Time ASC;`
	if ownerStmt == nil {
		ownerStmt, err = conn.Prepare(sql)
		if err != nil {
			return nil, nil, err
		}
	}

	var owner, t int
	err = ownerStmt.Query(id)
	for err == nil {
		if err := ownerStmt.Scan(&owner, &t); err != nil {
			return nil, nil, err
		}
		owners = append(owners, owner)
		times = append(times, t)
		err = ownerStmt.Next()
	}
	if err != io.EOF {
		return nil, nil, err
	}
	return owners, times, nil
}

func CreateIndex(conn *sqlite3.Conn) error {
	sql := `CREATE INDEX IF NOT EXISTS res_id ON Resources(ID ASC);
	        CREATE INDEX IF NOT EXISTS res_par1 ON Resources(Parent1 ASC);
	        CREATE INDEX IF NOT EXISTS res_par2 ON Resources(Parent2 ASC);
	        CREATE INDEX IF NOT EXISTS trans_id ON Transactions(ID ASC);
	        CREATE INDEX IF NOT EXISTS trans_time ON Transactions(Time ASC);
	        CREATE INDEX IF NOT EXISTS trans_receiver ON Transactions(ReceiverID ASC);
	        CREATE INDEX IF NOT EXISTS transres_transid ON TransactedResources(TransactionID ASC);
	        CREATE INDEX IF NOT EXISTS transres_resid ON TransactedResources(ResourceID ASC);
	        CREATE INDEX IF NOT EXISTS rescreate_resid ON ResCreators(ResID ASC);`
	return conn.Exec(sql)
}

func fatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

