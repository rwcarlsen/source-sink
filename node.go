
package main

import (
	"fmt"
)

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
	Id       int32
	Time     int32
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

func (n *Node) Changes(added, removed map[int32]map[*Node]bool) {
	n.changes(added, removed)
}

func (n *Node) changes(added, removed map[int32]map[*Node]bool) {
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

func (n *Node) ChoppedLeaves(choptime int32) []*Node {
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

func (n *Node) IsLeaf(choptime int32) bool {
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

