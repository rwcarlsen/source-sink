package main

import (
	"fmt"

	"code.google.com/p/go-sqlite/go1/sqlite3"
)

func CreateIndex(conn *sqlite3.Conn) error {
	sql := "CREATE INDEX IF NOT EXISTS res_par1 ON Resources(Parent1 ASC);"
	sql += "CREATE INDEX IF NOT EXISTS res_par2 ON Resources(Parent2 ASC);"
	sql += "CREATE INDEX IF NOT EXISTS trans_id ON Transactions(ID ASC);"
	sql += "CREATE INDEX IF NOT EXISTS transres_transid ON TransactedResources(TransactionID ASC);"
	sql += "CREATE INDEX IF NOT EXISTS trans_sender ON Transactions(SenderID ASC);"
	sql += "CREATE INDEX IF NOT EXISTS trans_receiver ON Transactions(ReceiverID ASC);"
	return conn.Exec(sql)
}

// map[id]node
var all map[int32]*Node
// map[id]par1
var par1 map[int32]int32
// map[id]par2
var par2 map[int32]int32

func BuildHeritageHT(conn *sqlite3.Conn) (allNodes map[int32]*Node, err error) {
	if all != nil {
		return all, nil
	}

	all = make(map[int32]*Node, 100000)
	par1 = make(map[int32]int32, 1000000)
	par2 = make(map[int32]int32, 100000)
	fmt.Println("building tree...")

	sql := "SELECT ID,TimeCreated,Parent1,Parent2 FROM Resources;"
	for stmt, err := conn.Query(sql); err == nil; err = stmt.Next() {
		id, p1, p2, t := 0, 0, 0, 0
		if err := stmt.Scan(&id, &t, &p1, &p2); err != nil {
			return nil, err
		}
		node := &Node{Id: int32(id), Time: int32(t)}
		all[node.Id] = node
		if p1 != 0 {
			par1[node.Id] = int32(p1)
		}
		if p2 != 0 {
			par2[node.Id] = int32(p2)
		}
	}
	fmt.Println("connecting tree nodes...")
	for id, node := range all {
		if p1, ok := par1[id]; ok {
			all[p1].AddChild(node)
		}
		if p2, ok := par2[id]; ok {
			all[p2].AddChild(node)
		}
	}
	fmt.Println("tree finished.")

	return all, nil
}

func BuildAgentGraph(conn *sqlite3.Conn, agentId int32) (roots []*Node, err error) {
	if err := CreateIndex(conn); err != nil {
		return nil, err
	}

	allNodes, err := BuildHeritageHT(conn)
	if err != nil {
		return nil, err
	}

	// get all resources transacted to/from an agent and when the tx occured
	sql := `SELECT trr.ResourceID,tr.ReceiverID FROM Transactions AS tr
           INNER JOIN TransactedResources AS trr
             ON tr.ID = trr.TransactionID
           WHERE tr.SenderID = ? OR tr.ReceiverID = ?;`

	inNodes := []*Node{}
	for stmt, err := conn.Query(sql, agentId, agentId); err == nil; err = stmt.Next() {
		receiverId := 0
		resourceId := 0
		if err := stmt.Scan(&resourceId, &receiverId); err != nil {
			return nil, err
		}
		if int32(receiverId) == agentId {
			inNodes = append(inNodes, allNodes[int32(resourceId)])
		}
	}
	return inNodes, nil
}

func ListAgents(conn *sqlite3.Conn) ([]int, error) {
	ids := []int{}
	for stmt, err := conn.Query("SELECT ID FROM Agents"); err == nil; err = stmt.Next() {
		id := 0
		if err := stmt.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func TimeInventory(conn *sqlite3.Conn, roots []*Node) (map[int32]map[*Node]bool, error) {
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
	inventory := map[int32]map[*Node]bool{}
	for i := start; i < start+dur; i++ {
		inventory[int32(i)] = map[*Node]bool{}
		for _, node := range roots {
			leaves := node.ChoppedLeaves(int32(i))
			for _, leaf := range leaves {
				inventory[int32(i)][leaf] = true
			}
		}
	}
	return inventory, nil
}
