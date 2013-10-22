package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strconv"
	"time"

	"code.google.com/p/go-sqlite/go1/sqlite3"
)

var fulltree = flag.Bool("res-tree", false, "output dot graph of entire resource tree")
var inven = flag.Bool("inventory", false, "print time series of agent's resource id inventory")
var changes = flag.Bool("changes", false, "print time series of changes to agent's resource id inventory")
var qty = flag.Bool("qty", false, "show quantities in dot graph")
var allAgents = flag.Bool("all", false, "do stuff for each agent")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var agentId int32

var conn *sqlite3.Conn

func main() {
	log.SetFlags(0)
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	fname := flag.Arg(0)

	var err error
	if flag.NArg() > 1 {
		v, err := strconv.Atoi(flag.Arg(1))
		agentId = int32(v)
		fatal(err)
	}

	conn, err = sqlite3.Open(fname)
	fatal(err)
	defer conn.Close()

	if *fulltree {
		outputFullTree()
	} else if *allAgents {
		outputAllAgents()
	} else if *inven {
		outputTimeInventory()
	} else if *changes {
		outputChanges()
	} else {
		go outputAgentGraph()
	}

	<-time.After(60 * time.Second)
}

func outputAllAgents() {
	ids, err := ListAgents(conn)
	fatal(err)

	for i, id := range ids {
		agentId = int32(id)
		fmt.Printf("building graph %v for agent %v\n", i, id)
		outputAgentGraph()
	}
}

func outputAgentGraph() {
	roots, err := BuildAgentGraph(conn, agentId)
	fatal(err)

	edges := EdgeSet{}
	for _, node := range roots {
		edges.Union(node.DotEdges())
	}
	title := fmt.Sprintf("agent_%v", agentId)
	fmt.Println(BuildDot(title, edges.Slice()))
}

func outputChanges() {
	roots, err := BuildAgentGraph(conn, agentId)
	fatal(err)

	added := map[int32]map[*Node]bool{}
	removed := map[int32]map[*Node]bool{}
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
}

func outputTimeInventory() {
	roots, err := BuildAgentGraph(conn, agentId)
	fatal(err)

	inventory, err := TimeInventory(conn, roots)
	fatal(err)
	for t, set := range inventory {
		fmt.Printf("timestep %v\n", t)
		for node, _ := range set {
			fmt.Printf("    %s\n", node)
		}
	}
}

func outputFullTree() {
	//roots, err := BuildResTree(conn)
	//fatal(err)

	//edges := EdgeSet{}
	//for _, node := range roots {
	//	edges.Union(node.DotEdges())
	//}
	//fmt.Println(BuildDot("ResourceTree", edges.Slice()))
}

func BuildDot(title string, edges []string) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "digraph %v {\n", title)
	for _, edge := range edges {
		fmt.Fprintf(&buf, "    %v;\n", edge)
	}
	buf.WriteString("}")
	return buf.String()
}

func fatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
