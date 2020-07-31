import concurrent.futures as con
import re
import socket
import threading


class Node:
    """ Nodes in system """

    def __init__(self, node_id="", ip="", udp_port=0):
        self.node_id = node_id
        self.ip = ip
        self.udp_port = udp_port
        self.cluster = []

    def add_to_cluster(self, c_node):
        self.cluster.append(c_node)

    def init_cluster(self, file_name):
        with open("../" + file_name, "r") as f:
            n = [re.split("\\s+", line.rstrip('\n')) for line in f]

            self.node_id, self.ip, self.udp_port = n[0][0], n[0][1], int(n[0][2])
            for i in range(1, len(n)):
                self.add_to_cluster(Node(n[i][0], n[i][1], int(n[i][2])))

    def merge(self, new_cluster):
        """
        Deserialized cluster string and merge it into current cluster
        :param new_cluster: serialized cluster string
        :return: None
        """
        n = [re.split("\\s+", line.rstrip('\n')) for line in new_cluster]
        for i in n:
            nc = Node(i[0], i[1], int(i[2]))
            if not self.cluster.__contains__(nc) and not nc == self:
                self.cluster.append(nc)


def init_node():
    node_num = input("Enter init to start. Enter '-l' and text file name to from file: ").split()
    _node = Node()

    if node_num.__len__() > 1:  # to read cluster and IP from file
        _node.init_cluster(node_num[2])
    else:
        _node.node_id, _node.ip = input("Enter name & IP: ").split()
        _node.udp_port = int(input("Enter UDP port: "))
        while True:
            data = input("Enter node name, IP and UDP port. Enter '0' to finish: ").split()
            if data[0] == '0':
                break
            _node.add_to_cluster(Node(data[0], data[1], int(data[2])))

    return _node


def send_cluster():
    """ to send cluster list to cluster nodes (sending cluster and itself serialized) """
    # serializing cluster
    ser = ""
    for c in node.cluster:
        ser = ser + c.node_id + " " + c.ip + " " + str(c.udp_port) + "\n"
    ser = ser + node.node_id + " " + node.ip + " " + str(node.udp_port)

    for c in node.cluster:
        ucs.sendto(bytes(ser, encoding="UTF-8"), (c.ip, c.udp_port))

    threading.Timer(interval, send_cluster).start()
    print("Cluster Sent")


def udp_server():
    """ to get cluster lists and merge them """
    while True:
        print("asevgsv")
        rec_data, addr = uss.recvfrom(1024)
        node.merge(str(rec_data, encoding="UTF-8"))


if __name__ == '__main__':
    node = init_node()

    interval = 1.0  # float(input("Enter Discovering interval: "))  # discover interval
    waiting = 5.0  # float(input("Enter Waiting for Respond time: "))
    service_num = 5  # int(input("Enter Number of Concurrent Services: "))

    # UDP server socket
    uss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    uss.bind((node.ip, node.udp_port))
    with con.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(udp_server, range(5))

    # UDP client socket
    ucs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    send_cluster()

    # main loop
