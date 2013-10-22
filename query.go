package main

import (
	"io"

	"code.google.com/p/go-sqlite/go1/sqlite3"
)

func CreateIndex(conn *sqlite3.Conn) error {
	sql := "CREATE INDEX IF NOT EXISTS res_par1 ON Resources(Parent1 ASC);"
	sql += "CREATE INDEX IF NOT EXISTS res_par2 ON Resources(Parent2 ASC);"
	sql += "CREATE INDEX IF NOT EXISTS trans_id ON Transactions(ID ASC);"
	sql += "CREATE INDEX IF NOT EXISTS trans_sender ON Transactions(SenderID ASC);"
	sql += "CREATE INDEX IF NOT EXISTS trans_receiver ON Transactions(ReceiverID ASC);"
	sql += "CREATE INDEX IF NOT EXISTS transres_transid ON TransactedResources(TransactionID ASC);"
	return conn.Exec(sql)
}

func BuildResTree(conn *sqlite3.Conn) (roots []*Node, err error) {
	if err := CreateIndex(conn); err != nil {
		return nil, err
	}

	// create root and end nodes
	sql := "SELECT ID,TimeCreated,Quantity,Parent1,Parent2 FROM Resources"
	for stmt, err := conn.Query(sql); err == nil; err = stmt.Next() {
		p1, p2 := 0, 0
		if err := stmt.Scan(nil, nil, nil, &p1, &p2); err != nil {
			return nil, err
		}
		if p1 == 0 && p2 == 0 {
			root := &Node{}
			if err := stmt.Scan(&root.Id, &root.Time, &root.Qty); err != nil {
				return nil, err
			}
			roots = append(roots, root)
			addChildren(conn, root, map[int]bool{})
		}
	}

	return roots, nil
}

func BuildAgentGraph(conn *sqlite3.Conn, agentId int) (roots []*Node, err error) {
	if err := CreateIndex(conn); err != nil {
		return nil, err
	}

	// get all resources transacted to/from an agent and when the tx occured
	sql := `SELECT trr.ResourceID,tr.Time,tr.ReceiverID FROM Transactions AS tr
           INNER JOIN TransactedResources AS trr
             ON tr.ID = trr.TransactionID
           WHERE tr.SenderID = ? OR tr.ReceiverID = ?;`

	inNodes := []*Node{}
	outIds := map[int]bool{}
	var stmt *sqlite3.Stmt
	for stmt, err = conn.Query(sql, agentId, agentId); err == nil; err = stmt.Next() {
		var receiverId int
		var resourceId int
		var t int
		if err := stmt.Scan(&resourceId, &t, &receiverId); err != nil {
			return nil, err
		}
		if receiverId == agentId {
			inNodes = append(inNodes, &Node{Id: resourceId, Time: t})
		} else {
			outIds[resourceId] = true
		}
	}
	if err != io.EOF {
		return nil, err
	}

	// build tree(s) between root and end nodes
	for _, node := range inNodes {
		addChildren(conn, node, outIds)
	}
	return inNodes, nil
}

func addChildren(conn *sqlite3.Conn, node *Node, outIds map[int]bool) (err error) {
	if outIds[node.Id] {
		return nil
	}

	sql := "SELECT ID,TimeCreated FROM Resources WHERE Parent1 = ? OR Parent2 = ?;"
	var stmt *sqlite3.Stmt
	for stmt, err = conn.Query(sql, node.Id, node.Id); err == nil; err = stmt.Next() {
		child := &Node{}
		if err := stmt.Scan(&child.Id, &child.Time); err != nil {
			return err
		}

		if !outIds[child.Id] {
			addChildren(conn, child, outIds)
		}
		node.AddChild(child)
	}
	if err != io.EOF {
		return err
	}
	return nil
}

func ListAgents(conn *sqlite3.Conn) ([]int, error) {
	ids := []int{}
	for stmt, err := conn.Query("SELECT ID FROM Agents"); err == nil; err = stmt.Next() {
		v := 0
		if err := stmt.Scan(&v); err != nil {
			return nil, err
		}
		ids = append(ids, v)
	}
	return ids, nil
}

func TimeInventory(conn *sqlite3.Conn, roots []*Node) (map[int]map[*Node]bool, error) {
	// find simulation duration
	stmt, err := conn.Query("SELECT SimulationStart,Duration FROM SimulationTimeInfo")
	if err != nil {
		return nil, err
	}

	start, dur := 0, 0
	err = stmt.Scan(&start, &dur)
	if err != nil {
		return nil, err
	}

	// find cumulative leaf resource id's for each timestep
	inventory := map[int]map[*Node]bool{}
	for i := start; i < start+dur; i++ {
		inventory[i] = map[*Node]bool{}
		for _, node := range roots {
			leaves := node.ChoppedLeaves(i)
			for _, leaf := range leaves {
				inventory[i][leaf] = true
			}
		}
	}
	return inventory, nil
}
