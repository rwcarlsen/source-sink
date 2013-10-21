
import sqlite3

def main():
  agent_id = '5'
  fname = 'out.sqlite'

  conn = sqlite3.connect(fname)
  conn.row_factory = sqlite3.Row

  nodes = build_trees(conn, agent_id)
  #nodes = build_full(conn)

  # print dot graph
  edges = set()
  for node in nodes:
    edges |= node.dot_edges()
  print(dot_graph(edges))
  return
  
  # print time inventories
  inventory = time_inventory(conn, nodes)
  for key, val in inventory.items():
    print('timestep', key)
    for node in val:
      print('    ', node)

def time_inventory(conn, root_nodes):
  # find simulation duration
  q = conn.execute('SELECT SimulationStart,Duration FROM SimulationTimeInfo')
  row = q.fetchone()
  end_time = row['SimulationStart'] + row['Duration']

  # find cumulative leaf resource id's for each timestep
  inventory = {}
  for i in range(0, end_time):
    inventory[i] = set()
    for node in root_nodes:
      leaves = node.chopped_leaves(i)
      inventory[i] |= set(leaves)
  return inventory

def dot_graph(edges):
  s = 'digraph G {\n'
  for edge in edges:
    s += '    ' + edge + ';\n'
  s += '\n}'
  return s

def build_full(conn):
  """
  Builds resource heritage tree for the entire simulation including all
  agents and returns root nodes.
  """
  # set up connection and create indexes
  conn.execute('CREATE INDEX IF NOT EXISTS res_par1 ON Resources(Parent1 ASC)')
  conn.execute('CREATE INDEX IF NOT EXISTS res_par2 ON Resources(Parent2 ASC)')

  # create root and end nodes
  in_nodes = []
  out_ids = []
  sql =  'SELECT Parent1,Parent2,ID,TimeCreated,Quantity FROM Resources'
  for row in conn.execute(sql):
    if row['Parent1'] == 0 and row['Parent2'] == 0:
      in_nodes.append(Node(row['ID'], row['TimeCreated'], row['Quantity']))

  # build tree(s) between root and end nodes
  for node in in_nodes:
    add_children(conn, node, out_ids)

  return in_nodes

def build_trees(conn, agent_id):
  """
  Builds a set of resource heritage trees for resources inside agent_id
  between the specified start and end times.  These trees are not
  necessarily disjoint. The returned value is the list of root nodes.
  """
  # set up connection and create indexes
  conn.execute('CREATE INDEX IF NOT EXISTS res_par1 ON Resources(Parent1 ASC)')
  conn.execute('CREATE INDEX IF NOT EXISTS res_par2 ON Resources(Parent2 ASC)')

  # get all resources transacted to/from an agent and when the tx occured
  sql =  'SELECT trr.ResourceID,tr.Time,tr.SenderID,tr.ReceiverID,res.Quantity FROM Transactions AS tr'
  sql += ' INNER JOIN TransactedResources AS trr'
  sql +=   ' ON tr.ID = trr.TransactionID'
  sql += ' INNER JOIN Resources AS res'
  sql +=   ' ON trr.ResourceID = res.ID'
  sql += ' WHERE tr.SenderID = ' + agent_id + ' OR tr.ReceiverID = ' + agent_id

  in_nodes = []
  out_ids = []
  for row in conn.execute(sql):
    if str(row['ReceiverID']) == agent_id:
      in_nodes.append(Node(row['ResourceID'], row['Time'], row['Quantity']))
    else:
      out_ids.append(str(row['ResourceID']))

  # build tree(s) between root and end nodes
  for node in in_nodes:
    add_children(conn, node, out_ids)

  return in_nodes

def add_children(conn, node, out_ids):
  if node.res_id in out_ids:
    return

  sql = 'SELECT ID,TimeCreated,Quantity FROM Resources'
  sql += ' WHERE Parent1 = ' + node.res_id + ' OR Parent2 = ' + node.res_id
  add_funcs = [node.set_left, node.set_right]
  for i, row in enumerate(conn.execute(sql)):
    child = Node(row['ID'], row['TimeCreated'], row['Quantity'])
    add_funcs[i](child)
    if node.res_id not in out_ids:
      add_children(conn, child, out_ids)

class Node:
  def __init__(self, res_id, time, qty):
    self.res_id = str(res_id)
    self.time = time
    self.qty = qty
    self.left = None
    self.right = None
    self.parent = None

  def __str__(self):
    return 'Res ' + self.res_id + ' @ t=' + str(self.time) + ' (qty ' + str(self.qty) + ')'

  def __eq__(self, other):
    return self.res_id == other.res_id

  def __ne__(self, other):
    return self.res_id != other.res_id

  def __hash__(self):
    return int(self.res_id)

  def dot_edges(self):
    edges = set()
    if self.left is not None:
      edges.add('"' + str(self) + '" -> "' + str(self.left) + '"')
      edges |= self.left.dot_edges()
    if self.right is not None:
      edges.add('"' + str(self) + '" -> "' + str(self.right) + '"')
      edges |= self.right.dot_edges()
    return edges

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
    if self.is_leaf(choptime):
      leaves.append(self)
      return leaves
    if self.time >= choptime:
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

    if (self.left is not None) and self.left.time < choptime:
      return False
    if (self.right is not None) and self.right.time < choptime:
      return False
    if self.time >= choptime:
      return False
    return True

if __name__ == '__main__':
  main()
