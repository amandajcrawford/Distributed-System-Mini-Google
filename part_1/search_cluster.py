import asyncio
import os
import pathlib
import shutil
import queue
import selectors
import socket
import sys
import numpy as np
import math
import string
from multiprocessing import Process, Queue, current_process
from base import MasterNode, WorkerNode, MessageBuilder, MessageParser, logger

class SearchWorkerNode(WorkerNode):
    # OUTCOME
    FOUND = 3
    NOT_FOUND = 4

    # Task States
    COMPLETED = 5
    NOT_COMPLETED = 6
    def __init__(self, host, port, master_addr):
        self.keyword_list = []
        self.search_tasks = Queue() # list of search requests that needs to be fulfilled
        self.curr_task = {} # current task that is being fullfilled
        return super(SearchWorkerNode, self).__init__(host, port, master_addr)

    def start_worker(self):
        self.task_status = self.NOT_COMPLETED

    def handle_request(self, conn, addr, received):

        # Parse Message
        received = received.decode("utf-8")
        parser = MessageParser()
        parsed = parser.parse(received)

        # Get keyword list of words
        if parsed.keywords:
            self.handle_keyword(parsed)

    def handle_keyword(self, parsed):
        logger.info('Searching directory for %s '%parsed.keywords)


class SearchMasterNode(MasterNode):
    def __init__(self, host, port, worker_num, index):
        self.index_dir = index
        # map of clients connections who are awaiting a response
        self.waiting_clients = {}
        self.work_queue = Queue()

        # Keep tabs on the number of task each worker is responsible for
        # key => port, value => { key => job_id, value => keyword list }
        self.worker_assignments = {}
        return super(SearchMasterNode, self).__init__(host, port, worker_num)

    def start_master(self):
        logger.info('Starting Search Query Master')

    def handle_request(self, conn, addr, received):
        # Call this first to make sure worker nodes are being added to list
        super().handle_request(conn, addr, received)

        if self.worker_status == self.ALL_CONNECTED:
            logger.info('All Worker Nodes Are Connected!')

        # Parse the message received from sender
        # To add new message types to be sent update the MessageBuilder Class
        # Also update the MessageParser class to get the values
        received = received.decode("utf-8")
        parser = MessageParser()
        parsed = parser.parse(received)

        if parsed.type == 'client':
            self.handle_client_message(parsed)
        if parsed.type == 'worker':
            self.handle_worker_message(parsed)

        # Assign each worker with a responsible subset of keywords
        #self.assign_worker_keyword_split(self, conn, received)



        # Check to see if we have all the workers connected and ready
        #logger.info("Worker Status %s"%self.worker_status)


    def handle_client_message(self, parsed):
        pass

    def handle_worker_message(self, parsed):
        pass

    def assign_keyword_split(self):
       pass


class SearchCluster:
    '''
        SearchCluster: Instantiates a search cluster that instantiates a Search Query Master that is available to
        receive client keyword requests and Search Helper Worker Nodes that are available to assist with searching for keywords at
        any moment.

        TODO:
        Takes an initial set of workers but is able to expand......????
     '''

    def __init__(self, master_addr, worker_num, index_dir):
        if not master_addr:
            self.master_addr = ("localhost", 8956)
        else:
            self.master_addr = master_addr

        if not worker_num:
            return RuntimeError("Worker nodes not defined")

        self.host = self.master_addr[0]
        self.master_port = self.master_addr[1]
        self.nodes = {"master": master_addr, "workers": []}
        self.worker_num = worker_num
        self.index_dir = index_dir

    def start(self):
        nodes = []
        # Create Master Node and start process
        master = SearchMasterNode(self.master_addr[0], self.master_addr[1] , self.worker_num, self.index_dir)
        nodes.append(master)
        master.start()

        # Load addresses for worker nodes
        addr_list = []
        with open(os.path.join(os.path.dirname(__file__), 'search_hosts.txt'), 'r') as f:
            for line in f:
                l = line.strip().split(' ')
                addr = (l[0], int(l[1]))
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    res = sock.connect_ex(addr)
                    if res != 0: # Port is not in use by another process
                        addr_list.append(addr) # add as available

        # Start worker nodes and append to list
        worker_addr = []
        for i in range(self.worker_num):
            addr = addr_list[i]
            worker_addr.append(addr)
            host = addr[0]
            port = int(addr[1])
            node = SearchWorkerNode(host, port, self.master_addr)
            nodes.append(node)
            node.start()

        # Set worker addresses
        self.nodes['workers'] = worker_addr
        for node in nodes:
            node.join()


class SearchClient:
    def __init__(self, index_dir, num_nodes, host='localhost', port=9890):
        self.num_nodes = num_nodes
        self.index_dir = index_dir
        self.master_host = host
        self.master_port = int(port)

    def start(self):
        ip = (self.master_host, self.master_port)
        search = SearchCluster(ip, self.num_nodes, self.index_dir)
        search.start()


if __name__ == "__main__":
   input_dir = os.path.join(os.path.dirname(os.path.abspath(__name__)),'indexer/index')
   search = SearchClient(input_dir, 2, host='localhost', port=9860)
   search.start()
