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
        self.tcp_port = 0
        self.cluster = []
        self.cluster_id = []
        self.folder = ""
        self.mutex = False

    def init_cluster(self, file_name):
        with open("../" + file_name, "r") as f:
            n = [re.split("\\s+", line.rstrip('\n')) for line in f]

            self.node_id, self.ip, self.udp_port = n[0][0], n[0][1], int(n[0][2])
            self.folder = "../" + n[0][0]
            for i in range(1, len(n)):
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
    node_num = input("Enter init to start. Enter '-l' and text file name to from file: ").split()
    _node = Node()

    if len(node_num) > 1:  # to read cluster and IP from file
        _node.init_cluster(node_num[2])
    else:
        _node.node_id, _node.ip = input("Enter name & IP: ").split()
        _node.udp_port = int(input("Enter UDP port: "))
        _node.folder = input("Enter folder: ")
        while True:
            data = input("Enter node name, IP and UDP port. Enter '0' to finish: ").split()
            if data[0] == '0':
                break
            _node.cluster.append(Node(data[0], data[1], int(data[2])))
            _node.cluster_id.append(data[0])

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
        # print("Cluster Sent")


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
            data = rec_str.split()
            for root, dirs, files in os.walk(node.folder):
                if data[1] in files:  # file exists
                    ucs.sendto(bytes("FOUND " + " " + node.node_id + " " + node.ip + " " + str(node.tcp_port) + " " +
                                     str(datetime.now().time().second) + " " + str(datetime.now().time().microsecond),
                                     encoding="UTF-8"), (addr[0], int(data[2])))
        else:  # discovering
            node.merge(rec_str)


def sending_file():
    while True:
        c, addr = tss.accept()
        file_name = c.recv(1024)

        # sending file
        file = open(node.folder + "/" + str(file_name, encoding="UTF-8"), "rb")
        data = file.read(4096)
        while data:
            c.send(data)
            data = file.read(4096)

        file.close()
        c.close()


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
    with open(node.folder + "/" + file_name, "wb") as file:  # file we want to create
        data = tcs.recv(4096)
        while data:
            file.write(data)
            data = tcs.recv(4096)
            print("#", end="")
    print("\nDone!\n> ", end="")

    file.close()
    tcs.close()


if __name__ == '__main__':
    node = init_node()

    interval = 5.0  # float(input("Enter Discovering interval: "))  # discover interval
    waiting = 5.0  # float(input("Enter Waiting for Respond time: "))
    service_num = 5  # int(input("Enter Number of Concurrent Services: "))

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
    tss.listen(1)
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
                print("There is no cluster list.")
            else:
                for cn in node.cluster:
                    print(cn.node_id + " " + cn.ip)
        elif il.startswith("get"):
            fn = il.split()[1]  # file name
            rna.clear()
            for cn in node.cluster:
                ucs.sendto(bytes("GET " + fn + " " + str(node.udp_port), encoding="UTF-8"),
                           (cn.ip, cn.udp_port))

            # after waiting time
            threading.Timer(waiting, getting_file, [fn]).start()
        elif il == "exit":
            print("Goodbye!")
            os._exit(0)
        else:
            print("Enter a valid command.")
