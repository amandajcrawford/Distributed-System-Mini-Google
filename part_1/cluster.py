import logging 
import socket 
import multiprocessing

class Cluster():

    Clustered = 2
    Non_Clustered = 3

    Started = 4
    Finished = 5

    def __init___(self, master_node=None, worker_nodes=None, master_func=None, worker_func=None):
        if not master_node:
            master_node = ("localhost", 8956)
        if not worker_nodes:
            return RuntimeError('Worker nodes not defined')
        
        self.cluster_status = self.Non_Clustered
        self.nodes = {'master': master_node, 'workers': worker_nodes}

    def start(self):
        # Create Master Node'
        master = self.nodes[self.nodes['master']]
        self.master = MasterNode(master)
        
        # Create Worker Nodes
        self.workers = {}
        for worker in self.nodes['workers']:
            node = WorkerNode(worker)
            self.workers[worker] = node


class Node:
     def __init__(self, ip):
        self.ip = ip

class WorkerNode(Node):
    pass

class MasterNode(Node):
    pass

if __name__ == '__main__':
    master_node = ("localhost", 7548)
    worker_nodes = [("localhost", 8761), ("localhost", 8762), ("localhost", 8763)]
    cluster = Cluster(master_node, worker_nodes, None, None)
    cluster.start()