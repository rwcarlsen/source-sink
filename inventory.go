package main

import (
	"os"
	"bytes"
	"flag"
	"fmt"
	"log"
	"strconv"

	"code.google.com/p/go-sqlite/go1/sqlite3"
)

var conn *sqlite3.Conn

type Node struct {
	Id int32
	Time int32
	Died int32
	OwnerId int32
}

func main() {
	log.SetFlags(0)
	flag.Parse()

	fname := flag.Arg(0)

	var err error
	if flag.NArg() > 1 {
		agentId, err = strconv.Atoi(flag.Arg(1))
		fatal(err)
	}

	conn, err = sqlite3.Open(fname)
	fatal(err)
	defer conn.Close()

	err = CreateIndex(conn)
	fatal(err)

	roots, err := GetRoots(conn)
	fatal(err)
	for _, root := range roots {
		nodes, err := WalkDown(conn, root)
		fatal(err)
		err = BuildInventoryTable(nodes)
		fatal(err)
	}
}

func GetRoots(conn *sqlite3.Conn) (roots []*Node, err error) {
	sql := "SELECT ID,TimeCreated FROM Resources WHERE Parent1 = 0 AND Parent2 = 0"
	var stmt *sqlite3.Stmt
	var id, t int
	for stmt, err = conn.Query(sql); err == nil; err = stmt.Next() {
		err := stmt.Scan(&id, &t)
		fatal(err)
		roots = append(roots, &Node{Id: int32(id), Time: int32(t)})
	}
	if err != io.EOF {
		return nil, err
	}
	return roots, nil
}

func WalkDown(conn *sqlite3.Conn, node *Node) (nodes []*Node, err error) {
	sql := "SELECT ID,TimeCreated FROM Resources WHERE Parent1 = ? OR Parent2 = ?"
	var stmt *sqlite3.Stmt
	var id, t int
	for stmt, err = conn.Query(sql, node.Id, node.Id); err == nil; err = stmt.Next() {
		if err := stmt.Scan(&id, &t); err != nil {
			return nil, err
		}
		node.Died = int32(t)
		child := &Node{Id: int32(id), Time: int32(id)}

		owners, err := GetOwners(conn, node.Id)
		if err != nil {
			return nil, err
		}
		if len(owners) == 0 {
			nodes = append(nodes, node)
			child.OwnerId = node.OwnerId
		} else {
			var curr int32
			for _, curr = range owners {
				nodes = append(nodes, &Node{Id: node.Id, Time: node.Time, Died: node.Died OwnerId: curr})
			}
			child.OwnerId = curr
		}

		nodes = append(nodes, WalkDown(conn, child)...)
	}
	return nodes, nil
}

func GetOwners(conn *sqlite3.Conn, id int32) (owners []int32, err error) {
	// get all resources transacted to/from an agent and when the tx occured
	sql := `SELECT trr.ResourceID,tr.Time,tr.ReceiverID FROM Transactions AS tr
           INNER JOIN TransactedResources AS trr
             ON tr.ID = trr.TransactionID
           WHERE tr.SenderID = ? OR tr.ReceiverID = ?;`

	inNodes := []*Node{}
	outIds := map[int]bool{}
	var stmt *sqlite3.Stmt
	for stmt, err = conn.Query(sql, agentId, agentId); err == nil; err = stmt.Next() {
}

func CreateIndex(conn *sqlite3.Conn) error {
	sql := "CREATE INDEX IF NOT EXISTS res_par1 ON Resources(Parent1 ASC);"
	sql += "CREATE INDEX IF NOT EXISTS res_par2 ON Resources(Parent2 ASC);"
	sql += "CREATE INDEX IF NOT EXISTS trans_id ON Transactions(ID ASC);"
	sql += "CREATE INDEX IF NOT EXISTS trans_sender ON Transactions(SenderID ASC);"
	sql += "CREATE INDEX IF NOT EXISTS trans_receiver ON Transactions(ReceiverID ASC);"
	sql += "CREATE INDEX IF NOT EXISTS transres_transid ON TransactedResources(TransactionID ASC);"
	return conn.Exec(sql)
}

func fatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

