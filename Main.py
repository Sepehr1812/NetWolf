import re


class Node:
    """ Nodes in system """

    def __init__(self, node_id="", ip=""):
        self.node_id = node_id
        self.ip = ip
        self.udp_port = 0
        self.cluster = []

    def add_to_cluster(self, c_node):
        self.cluster.append(c_node)

    def init_cluster(self, file_name):
        with open("../" + file_name, "r") as f:
            n = [re.split("\\s+", line.rstrip('\n')) for line in f]

            self.node_id, self.ip = n[0]
            for i in range(1, len(n)):
                self.add_to_cluster(Node(n[i][0], n[i][1]))


def init_node():
    node_num = input("Enter init to start. Enter '-l' and text file name to from file: ").split()
    _node = Node()

    if node_num.__len__() > 1:  # to read cluster and IP from file
        _node.init_cluster(node_num[2])
    else:
        _node.node_id, _node.ip = input("Enter ID & IP: ").split()
        while True:
            data = input("Enter user ID number and IP. Enter '0' to finish: ").split()
            if data[0] == '0':
                break
            _node.add_to_cluster(Node(data[0], data[1]))

    _node.udp_port = int(input("Enter UDP port for this user: "))

    return _node


if __name__ == '__main__':
    node = init_node()

    print("help: udp for setting UDP port, dt for setting discover time period,\n"
          "wt for setting get respond waiting time, gn for setting number of get responses,\n"
          "list for cluster list, get <file> for searching for file, help for help,\nex for exit.")
