
import sqlite3

def main():
  agent_id = '97'
  fname = 'cyclus.sqlite'
  start_time = 0
  end_time = 0

  conn = sqlite3.connect(fname)
  conn.row_factory = sqlite3.Row

  # get all resources transacted to/from an agent and when the tx occured
  sql =  'SELECT trr.ResourceID,tr.Time,tr.SenderID,tr.ReceiverID FROM Transactions AS tr'
  sql += ' INNER JOIN TransactedResources AS trr'
  sql +=   ' ON tr.ID = trr.TransactionID'
  sql += ' WHERE tr.SenderID = ' + agent_id + ' OR tr.ReceiverID = ' + agent_id

  
  # create root and end nodes
  in_nodes = []
  out_ids = []
  for row in conn.execute(sql):
    if row['SenderID'] == agent_id:
      in_nodes.append(Node(row['ResourceID'], row['Time']))
    else:
      out_ids.append(str(row['ResourceID']))

  # build tree(s) between root and end nodes
  for node in in_nodes:
    add_children(conn, node, out_ids)

  # find cumulative leaf resource id's for each timestep
  inventory = {}
  for i in range(start_time, end_time):
    inventory[i] = set()
    for node in in_nodes:
      leaves = node.chopped_leaves(i)
      inventory[i] = inventory[i] | set(leaves)

def add_children(conn, node, out_ids):
  if node.res_id in out_ids:
    return

  sql = 'SELECT ID,TimeCreated FROM Resources'
  sql += ' WHERE Parent1 = ' + node.res_id
  for row in conn.execute(sql):
    left = Node(row['ID'], row['TimeCreated'])
    node.set_left(left)
    if left.res_id not in out_ids:
      add_children(conn, left, out_ids)

  sql = 'SELECT ID,TimeCreated FROM Resources'
  sql += ' WHERE Parent2 = ' + node.res_id
  for row in conn.execute(sql):
    right = Node(row['ID'], row['TimeCreated'])
    node.set_right(right)
    if right.res_id not in out_ids:
      add_children(conn, right, out_ids)

class Node:
  def __init__(self, res_id, time):
    self.res_id = str(res_id)
    self.time = time
    self.left = None
    self.right = None
    self.parent = None

  def set_left(self, node):
    self.left = node
    node.parent = self

  def set_right(self, node):
    self.right = node
    node.parent = self

  def chopped_leaves(self, choptime):
    """
    Returns the resource nodes that are leaves if and only if the time has not
    progressed beyond choptime.
    """
    leaves = []
    if self.is_leaf():
      leaves.append(self)
      return leaves

    if self.left is not None:
      leaves.extend(self.left.chopped_leaves(choptime))
    if self.right is not None:
      leaves.extend(self.right.chopped_leaves(choptime))
    return leaves

  def is_leaf(self, choptime = 99999999):
    """
    Returns true if this node has no children through choptime
    """
    leaf = True
    if (self.left is not None) and self.left.time < choptime:
      leaf = False
    if (self.right is not None) and self.right.time < choptime:
      leaf = False
    if self.time < choptime:
      leaf = False
    return leaf

if __name__ == '__main__':
  main()
