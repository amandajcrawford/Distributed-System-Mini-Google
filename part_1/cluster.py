import logging
import socket
import multiprocessing
import os, time, math
import numpy as np


class Cluster:

    Clustered = 2
    Non_Clustered = 3

    Started = 4
    Finished = 5

    def __init___(
        self, master_node=None, worker_nodes=None, master_func=None, worker_func=None
    ):
        if not master_node:
            master_node = ("localhost", 8956)
        if not worker_nodes:
            return RuntimeError("Worker nodes not defined")

        self.cluster_status = self.Non_Clustered
        self.nodes = {"master": master_node, "workers": worker_nodes}

    def start(self):
        # Create Master Node'
        master = self.nodes[self.nodes["master"]]
        self.master = MasterNode(master)

        # Create Worker Nodes
        self.workers = {}
        for worker in self.nodes["workers"]:
            node = WorkerNode(worker)
            self.workers[worker] = node


class Node:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def __init__(self, ip, port):
        self.port = int(port)
        self.ip = ip
        self.sock.bind(("", port))
        self.sock.listen(5)


class WorkerNode(Node):
    # Data
    # task tracker
    # data node
    # my directory is storing a specific partition
    # mapper
    # reduce
    # shuffle

    # search

    pass


class MasterNode:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # The line above should change to a list containing the address of each worker
    # instead of a fix value, #workers will be -> workers = len(list)
    workers = 4

    def __init__(self, ip, port):
        self.port = int(port)
        self.ip = ip
        self.sock.bind((ip, port))
        self.sock.listen(5)

    def populateArrayOfFilesAndSize(self,path):
        files = os.listdir(os.path.abspath(path))
        arrayOfFilesAndSize = np.empty([1, 0])
        for iFile in files:
            pathToFile = os.path.join(
                path,
                iFile,
            )
            f = open(os.path.abspath(pathToFile), encoding="ISO-8859-1")
            lines = 0
            buf_size = 1024 * 1024
            read_f = f.read
            buf = read_f(buf_size)
            while buf:
                lines += buf.count("\n")
                buf = read_f(buf_size)
            f.close()
            arrayOfFilesAndSize = np.append(arrayOfFilesAndSize, [{pathToFile: lines}])
        return arrayOfFilesAndSize

    def distributeJobToMappers(self,arrayOfDictionariesOfFilesPaths):
        for index,i in enumerate(arrayOfDictionariesOfFilesPaths):
            k = i
            fi = list(k.keys())[0]
            k = list(k.values())[0]
            lines = k
            t = math.ceil(k/self.workers)
            worker,y = 0,0
            while (lines - t) > 0:
                k = y
                y += t
                print("Worker ",worker," will receive file: ",fi," starting at line ",k," ending at line ",y)
                lines-=t
                worker+=1
            # os.system("gnome-terminal -e 'bash -c \"python client.py %s %s %s %s; exec bash\"'"%(serverAddresses[i].get('ip'),serverAddresses[i].get('port'),y,sys.argv[2]))
            print("Worker ",worker," will receive file: ",fi," starting at line ",y," ending at line ",(list(i.values())[0]))

    def startServer(self):
        while True:
            c, addr = self.sock.accept()
            data, address = c.recvfrom(4096)
            request = data.decode()
            #Client request for indexing a given path
            #The request will be as follow:
            """
                RPC:/root/user/path/to/where/directory/is
            """
            if request[:3] == "RPC":
                arrayOfFilesAndSize = self.populateArrayOfFilesAndSize(request[4:])
                    # is it better to read then distribute the blocks, or distribute while reading?
                self.distributeJobToMappers(arrayOfFilesAndSize) 
            c.close()

    # communicated to the worker nodes - finding a new worker node
    # job manager

    # name node
    # schedule

    # search
    # pass


class IndexMaster:
    # Ordering of Events
    # 1. Cluster to worker nodes
    # 2. Load HDFS and populate name node data
    # partition input files
    # assign a worker node to specific partition ( Directory of DataNodes and Data Partitions)
    # give a task to the worker node to store
    # waits for the worker node to give a success for storing data
    # 3. Scheduling mappers (dependent partitions, fault tolerance, speedup)
    # 4. Schedule reducers
    # 5. Index Complete

    def JobTracker(self):
        pass

    def NameNode(self):
        # Port Number to handle hdfs requests
        pass


class SearchMaster:
    def getKeywords(self):
        pass

    def results():
        pass


if __name__ == "__main__":
    Master = MasterNode('127.0.0.1',45000)
    Master.startServer()
    # print(Master.port)
    # master_node = ("localhost", 7548)
    # worker_nodes = [("localhost", 8761), ("localhost", 8762), ("localhost", 8763)]
    # cluster = Cluster(master_node, worker_nodes, None, None)
    # cluster.start()

    """
    HDFS
    - Read the input files 
    - Create blocks of text size (128mb)
    - Store thorse

    """

