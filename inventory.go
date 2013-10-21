package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"strconv"

	"code.google.com/p/go-sqlite/go1/sqlite3"
)

var fulltree = flag.Bool("res-tree", false, "output dot graph of entire resource tree")
var inven = flag.Bool("inventory", false, "print time series of agent's resource id inventory")
var changes = flag.Bool("changes", false, "print time series of changes to agent's resource id inventory")
var qty = flag.Bool("qty", false, "show quantities in dot graph")

func main() {
	log.SetFlags(0)
	flag.Parse()

	fname := flag.Arg(0)
	agentId, err := strconv.Atoi(flag.Arg(1))
	fatal(err)

	conn, err := sqlite3.Open(fname)
	fatal(err)
	defer conn.Close()

	roots, err := BuildAgentGraph(conn, agentId)
	fatal(err)

	if *fulltree {
		roots, err := BuildResTree(conn)
		fatal(err)

		edges := EdgeSet{}
		for _, node := range roots {
			edges.Union(node.DotEdges())
		}
		fmt.Println(BuildDot(edges.Slice()))
	} else if *inven {
		inventory, err := TimeInventory(conn, roots)
		fatal(err)
		for t, set := range inventory {
			fmt.Printf("timestep %v\n", t)
			for node, _ := range set {
				fmt.Printf("    %s\n", node)
			}
		}
	} else if *changes {
		added := map[int]map[*Node]bool{}
		removed := map[int]map[*Node]bool{}
		for _, node := range roots {
			node.Changes(added, removed)
		}
		for time, set := range added {
			fmt.Printf("Added at timestep %v\n", time)
			for node, _ := range set {
				fmt.Printf("    %s\n", node)
			}
		}
		for time, set := range removed {
			fmt.Printf("Removed at timestep %v\n", time)
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
	}
}

type EdgeSet map[string]bool

func (es EdgeSet) Slice() (edges []string) {
	for edge, _ := range es {
		edges = append(edges, edge)
	}
	return edges
}

func (es EdgeSet) Union(other EdgeSet) {
	for edge, _ := range other {
		es[edge] = true
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

func (n *Node) Changes(added, removed map[int]map[*Node]bool) {
	n.changes(added, removed)
}

func (n *Node) changes(added, removed map[int]map[*Node]bool) {
	if _, ok := added[n.Time]; !ok {
		added[n.Time] = map[*Node]bool{}
	}
	if _, ok := removed[n.Time]; !ok {
		removed[n.Time] = map[*Node]bool{}
	}

	added[n.Time][n] = true
	if n.Parent != nil {
		removed[n.Time][n.Parent] = true
	}
	for _, child := range n.Children {
		child.changes(added, removed)
	}
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

func CreateIndex(conn *sqlite3.Conn) error {
	sql := "CREATE INDEX IF NOT EXISTS res_par1 ON Resources(Parent1 ASC);"
	sql += "CREATE INDEX IF NOT EXISTS res_par2 ON Resources(Parent2 ASC);"
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

func fatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
