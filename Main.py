import re


class Node:
    """ Nodes in system """

    def __init__(self, node_id, ip=""):
        self.cluster = []
        self.node_id = node_id
        self.ip = ip

    def add_to_cluster(self, c_node):
        self.cluster.append(c_node)

    def init_cluster(self):
        with open("../c" + str(self.node_id) + ".txt", "r") as f:
            n = [re.split("\\s+", line.rstrip('\n')) for line in f]

            self.ip = n[0][0]
            for i in range(1, len(n)):
                self.add_to_cluster(Node(n[i][0], n[i][1]))


node_num = input("Enter user ID number. Enter '-l' after to read cluster file: ").split()
node = Node(node_num[0])

if node_num.__len__() > 1:  # to read from file
    node.init_cluster()
else:
    node.ip = input("Enter IP: ")
    while True:
        data = input("Enter user ID number and IP. Enter '0' to finish: ").split()
        if data[0] == '0':
            break
        node.add_to_cluster(Node(data[0], data[1]))

print(node.ip)
