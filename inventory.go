package main

import (
	"bytes"
	"flag"
	"fmt"
	"strconv"
	"log"

	"code.google.com/p/go-sqlite/go1/sqlite3"
)

var fulltree = flag.Bool("res-tree", false, "output dot graph of entire resource tree")
var inven = flag.Bool("inventory", false, "print time series of agent's resource id inventory")
var qty = flag.Bool("qty", false, "show quantities in dot graph")

type EdgeSet map[string]bool

func (es EdgeSet) Slice() (edges []string) {
	for edge, _ := range es{
		edges = append(edges, edge)
	}
	return edges
}

func (es EdgeSet) Union(other EdgeSet) {
	for edge, _ := range other{
		es[edge] = true
	}
}

func main() {
	log.SetFlags(0)
	flag.Parse()

	fname := flag.Arg(0)
	agentId, err := strconv.Atoi(flag.Arg(1))
	if err != nil {
		log.Fatal(err)
	}

	conn, err := sqlite3.Open(fname)
	if err != nil {
		log.Fatal(err)
	}

	roots, err := BuildAgentGraph(conn, agentId)
	if err != nil {
		log.Fatal(err)
	}

	if *inven {
		// print time inventories
		inventory, err := TimeInventory(conn, roots)
		if err != nil {
			log.Fatal(err)
		}
		for t, set := range inventory {
			fmt.Printf("timestep %v\n", t)
			for node, _ := range set {
				fmt.Printf("    %s\n", node)
			}
		}
	} else {
		// print dot graph
		edges := EdgeSet{}
		for _, node := range roots {
			edges.Union(node.DotEdges())
		}
		fmt.Println(BuildDot(edges.Slice()))
		return
	}
}

type Node struct {
	Id       int
	Time     int
	Qty      float64
	Parent   *Node
	Children []*Node
}

func (n *Node) String() string {
	return fmt.Sprintf("Res %v @ t=%v", n.Id, n.Time)
}

func (n *Node) DotEdges() EdgeSet {
	edges := EdgeSet{}
	for _, child := range n.Children {
		edges[fmt.Sprintf("\"%s\" -> \"%s\"", n, child)] = true
		edges.Union(child.DotEdges())
	}
	return edges
}

func (n *Node) AddChild(child *Node) {
	n.Children = append(n.Children, child)
	child.Parent = n
}

func (n *Node) ChoppedLeaves(choptime int) []*Node {
	leaves := []*Node{}
	if n.IsLeaf(choptime) {
		leaves = append(leaves, n)
		return leaves
	}
	if n.Time >= choptime {
		return leaves
	}

	for _, child := range n.Children {
		leaves = append(leaves, child.ChoppedLeaves(choptime)...)
	}
	return leaves
}

func (n *Node) IsLeaf(choptime int) bool {
	for _, child := range n.Children {
		if child.Time < choptime {
			return false
		}
	}
	if n.Time >= choptime {
		return false
	}
	return true
}

func BuildDot(edges []string) string {
	buf := bytes.NewBufferString("digraph G {\n")
	for _, edge := range edges {
		buf.WriteString(fmt.Sprintf("    %v;\n", edge))
	}
	buf.WriteString("}")
	return buf.String()
}

func BuildAgentGraph(conn *sqlite3.Conn, agentId int) (roots []*Node, err error) {
	// set up connection and create indexes
	sql := "CREATE INDEX IF NOT EXISTS res_par1 ON Resources(Parent1 ASC);"
	sql += "CREATE INDEX IF NOT EXISTS res_par2 ON Resources(Parent2 ASC);"
	if err := conn.Exec(sql); err != nil {
		return nil, err
	}

	// get all resources transacted to/from an agent and when the tx occured
	sql = `SELECT trr.ResourceID,tr.Time,tr.ReceiverID FROM Transactions AS tr
           INNER JOIN TransactedResources AS trr
             ON tr.ID = trr.TransactionID
           WHERE tr.SenderID = ? OR tr.ReceiverID = ?;`

	inNodes := []*Node{}
	outIds := map[int]bool{}
	for stmt, err := conn.Query(sql, agentId, agentId); err == nil; err = stmt.Next() {
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

	// build tree(s) between root and end nodes
	for _, node := range inNodes {
		addChildren(conn, node, outIds)
	}
	return inNodes, nil
}

func addChildren(conn *sqlite3.Conn, node *Node, outIds map[int]bool) error {
	if outIds[node.Id] {
		return nil
	}

	sql := "SELECT ID,TimeCreated FROM Resources WHERE Parent1 = ? OR Parent2 = ?;"
	for stmt, err := conn.Query(sql, node.Id, node.Id); err == nil; err = stmt.Next() {
		child := &Node{}
		if err := stmt.Scan(&child.Id, &child.Time); err != nil {
			return err
		}

		if !outIds[child.Id] {
			addChildren(conn, child, outIds)
		}
		node.AddChild(child)
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
