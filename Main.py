import os
import re
import socket
import threading
from datetime import datetime


class Node:
    """ Nodes in system """

    def __init__(self, node_id="", ip="", udp_port=0):
        self.node_id = node_id
        self.ip = ip
        self.udp_port = udp_port
        self.tcp_port: int
        self.cluster = []
        self.cluster_id = []
        self.folder = ""
        self.mutex = False
        self.good_nodes = []  # nodes that self has got files from them
        self.service_num: int  # number of concurrent services

    def init_cluster(self, file_name):
        with open("../" + file_name, "r") as f:
            n = [re.split("\\s+", line.rstrip('\n')) for line in f]
            for i in range(0, len(n)):
                self.cluster.append(Node(n[i][0], n[i][1], int(n[i][2])))
                self.cluster_id.append(n[i][0])

    def merge(self, new_cluster):
        """
        Deserialized cluster string and merge it into current cluster
        :param new_cluster: serialized cluster string
        :return: None
        """
        self.mutex = True  # to not send cluster while merging list

        lines = new_cluster.splitlines()
        for line in lines:
            i = line.split()
            if not self.cluster_id.__contains__(i[0]) and not self.node_id == i[0]:
                self.cluster.append(Node(i[0], i[1], int(i[2])))
                self.cluster_id.append(i[0])

        self.mutex = False


def init_node():
    _node = Node()

    _node.node_id, _node.ip = input("Enter name & IP: ").split()
    _node.udp_port = int(input("Enter UDP port: "))
    _node.folder = input("Enter folder: ")
    _node.init_cluster(input("Enter cluster list text file. It must be in the same level of the project folder: "))

    return _node


def send_cluster():
    """ to send cluster list to cluster nodes (sending cluster and itself serialized) """
    if not node.mutex:
        # serializing cluster
        ser = ""
        for c in node.cluster:
            ser = ser + c.node_id + " " + c.ip + " " + str(c.udp_port) + "\n"
        ser = ser + node.node_id + " " + node.ip + " " + str(node.udp_port)

        for c in node.cluster:
            ucs.sendto(bytes(ser, encoding="UTF-8"), (c.ip, c.udp_port))

        threading.Timer(interval, send_cluster).start()


def udp_server():
    """ to get cluster lists and merge them """
    while True:
        rec_data, addr = uss.recvfrom(4096)
        rec_str = str(rec_data, encoding="UTF-8")
        if rec_str.startswith("FOUND"):  # file exists
            data = rec_str.split()
            delay = (datetime.now().time().second - int(data[4])) * 1000000 + \
                    datetime.now().time().microsecond - int(data[5])
            rna.append(
                (delay, data[1], data[2], int(data[3])))  # respond tuples: delay, node ID, node IP, node TCP port
        elif rec_str.startswith("GET"):  # request for file file
            if node.service_num > 0:  # to control traffic distribution
                data = rec_str.split()
                for root, dirs, files in os.walk(node.folder):
                    if data[1] in files:  # file exists
                        micro = datetime.now().time().microsecond
                        if not node.good_nodes.__contains__(data[2]):  # to avoid free riding
                            micro -= 1000  # adding 1 ms to delay time

                        ucs.sendto(
                            bytes("FOUND " + " " + node.node_id + " " + node.ip + " " + str(node.tcp_port) + " " +
                                  str(datetime.now().time().second) + " " + str(micro), encoding="UTF-8"),
                            (data[3], int(data[4])))
        else:  # discovering
            node.merge(rec_str)


def sending_file():
    while True:
        c, addr = tss.accept()
        file_name = c.recv(1024)

        # sending file
        node.service_num -= 1

        file = open(node.folder + "/" + str(file_name, encoding="UTF-8"), "rb")
        data = file.read(4096)
        while data:
            c.send(data)
            data = file.read(4096)

        file.close()
        c.close()

        node.service_num += 1


def getting_file(file_name):
    # check if file found
    if len(rna) <= 0:
        print("File not found.\n> ", end="")
        return

    best_node = min(rna)

    # TCP client socket
    tcs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcs.connect((best_node[2], best_node[3]))
    tcs.send(bytes(file_name, encoding="UTF-8"))

    # receiving data
    print("Getting " + file_name + " from " + best_node[1] + "...")
    node.good_nodes.append(best_node[1])

    with open(node.folder + "/" + file_name, "wb") as file:  # file we want to create
        data = tcs.recv(4096)
        while data:
            file.write(data)
            data = tcs.recv(4096)
    print("Done!\n> ", end="")

    file.close()
    tcs.close()


if __name__ == '__main__':
    print("*** Welcome to NetWolf! ***\n")
    node = init_node()

    interval = float(input("Enter Discovering interval time: "))
    waiting = float(input("Enter Waiting for Respond time: "))
    node.service_num = int(input("Enter Number of Concurrent Services: "))

    # UDP server socket
    uss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    uss.bind((node.ip, node.udp_port))
    threading.Thread(target=udp_server).start()

    # UDP client socket
    ucs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    send_cluster()

    # TCP server socket
    tss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tss.bind(("", 0))
    tss.listen()
    node.tcp_port = tss.getsockname()[1]
    print("TCP port number: " + str(node.tcp_port))
    threading.Thread(target=sending_file).start()

    # responding node array
    rna = []

    # program loop
    print("help: `list` for cluster list, `get <file_name>` for get a file, `exit` for exit.")
    while True:
        il = input("> ")
        if il == "list":
            if len(node.cluster) <= 0:
                print("Cluster list has no node.")
            else:
                for cn in node.cluster:
                    print(cn.node_id + " " + cn.ip)
        elif il.startswith("get"):
            fn = il.split()[1]  # file name
            rna.clear()
            for cn in node.cluster:
                ucs.sendto(bytes("GET " + fn + " " + node.node_id + " " + node.ip + " " + str(node.udp_port),
                                 encoding="UTF-8"), (cn.ip, cn.udp_port))

            # after waiting time
            threading.Timer(waiting, getting_file, [fn]).start()
        elif il == "exit":
            print("Goodbye!")
            # noinspection PyProtectedMember
            os._exit(0)
        else:
            print("Enter a valid command.")
