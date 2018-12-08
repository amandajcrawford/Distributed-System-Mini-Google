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
from multiprocessing import Process, JoinableQueue, current_process
from base import MasterNode, WorkerNode, MessageBuilder, MessageParser, logger
from map_reduce import MapReduceProcess

class IndexWorkerNode(WorkerNode):
    # TASK
    FREE = 9 # Available to start another task
    MAP = 10
    REDUCE = 11

    # Task States
    COMPLETED = 5
    NOT_COMPLETED = 6
    def __init__(self, host, port, worker_num, map_dir, red_dir):
        self.map_dir = map_dir
        self.red_dir = red_dir
        return super(IndexWorkerNode, self).__init__(host, port, worker_num)

    def start_worker(self):
        self.curr_task = self.FREE
        self.task_status = self.NOT_COMPLETED
        # loop = asyncio.get_event_loop()
        # start map reduce process  that pulls from task queue
        task_queue = JoinableQueue()
        # self.map_reduce_engine = MapReduceProcess(task_queue)
        # self.map_reduce_engine.start()
        # self.map_reduce_engine.join()
        self.task_queue = task_queue

    def handle_request(self, conn, addr, received):

        # Parse Message
        received = received.decode("utf-8")
        parser = MessageParser()
        parsed = parser.parse(received)

        # Get task and add to queue
        task = parsed.action
        if task == 'map':
            self.handle_map_task(parsed)

        if task == 'reduce':
            self.handle_reduce_task(parsed)

    def handle_map_task(self, message):
        dir = message.map_dir,
        start = message.map_range_start,
        end = message.map_range_end
        mappers = 3
        task_obj = {
            task:'map',
            mappers: mappers,
            dir:self.map_dir,
            start:start,
            end: end
        }
        # May need to handle some setup work
        self.task_queue.put_nowait(task_obj)

    def handle_reduce_task(self, message):
        dir = message.red_dir,
        start = message.red_range_start,
        end = message.red_range_end
        reducers = 3

        task_obj = {
            task:'reduce',
            reducers: reducers,
            dir: self.red_dir,
            start:start,
            end: end
        }
        # May need to handle some  setup work
        self.task_queue.put_nowait(task_obj)


class IndexMasterNode(MasterNode):
    # Job Stages
    PARTITION = 1
    MAP = 2
    REDUCE = 3

    # Job Completion States
    NOT_STARTED = 4
    IN_PROGRESS = 5
    COMPLETED = 6

    # Overall Indexing States
    RUNNING = 7
    FINISHED = 8

    def __init__(self, host, port, worker_num, index):
        self.index_dir = index
        return super(IndexMasterNode, self).__init__(host, port, worker_num)

    def start_master(self):
        # Handles iterating through job stages
        self.curr_job = self.PARTITION #Partition or Map or Reduce
        self.job_status =  self.NOT_STARTED
        self.index_status = self.RUNNING

        # Directory where partitions are stored
        self.storage_dir = '\partions'

        logger.info('Starting Indexing Job')

    def handle_request(self, conn, addr, received):
        # Call this first to make sure worker nodes are being added to list
        super().handle_request(conn, addr, received)

        # Parse the message received from sender
        # To add new message types to be sent update the MessageBuilder Class
        # Also update the MessageParser class to get the values
        received = received.decode("utf-8")
        parser = MessageParser()
        parsed = parser.parse(received)

        # Check to see if we have all the workers connected and ready
        #logger.info("Worker Status %s"%self.worker_status)
        if self.worker_status == self.ALL_CONNECTED:
            if (self.curr_job == self.PARTITION) & (self.job_status == self.NOT_STARTED):
                logger.info('Partitioning Index Files For Worker Nodes')
                self.job_status = self.IN_PROGRESS
                arrayOfFilesAndSize = self.populateArrayOfFilesAndSize(self.index_dir)
                # is it better to read then distribute the blocks, or distribute while reading?
                # We should read then distribute
                self.distributeJobToMappers(arrayOfFilesAndSize)

    def populateArrayOfFilesAndSize(self, path):
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
        """
            Function receives an array of dictionaries where each dictionary has the form:
            'path/to/file': int number_of_lines}
            The function then distributes each block to each Mapper
        """
        worker_keys = list(self.worker_conns.keys())
        for index,i in enumerate(arrayOfDictionariesOfFilesPaths):
            k = i
            fi = list(k.keys())[0]
            k = list(k.values())[0]
            lines = k
            t = math.ceil(k/self.num_workers)
            worker,y = 0,0
            while (lines - t) > 0:
                k = y
                y += t
                print("Worker ",worker," will receive file: ",fi," starting at line ",k," ending at line ",y)
                lines-=t
                worker+=1
            print("Worker ",worker," will receive file: ",fi," starting at line ",y," ending at line ",(list(i.values())[0]))


            # # Create map task for worker
            # builder = MessageBuilder(messages=[])
            # builder.add_task_map_message(self.host, self.port, fi, y, (list(i.values())[0]) )
            # message = builder.build()

            # # Send task to worker by first getting the worker socket channel
            # worker_conn = self.worker_conns[worker_keys[worker]]
            # print(worker_conn)
            # logger.info('Sending worker message %s'% str(message.outb))
            # # Gets the converted outbound message and send to the worker
            # worker_conn.send(message.outb)

class IndexCluster:

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

    def setup_index_dir(self):
        # Create temp directory for map jobs
        self.index_map = os.path.join(os.path.dirname(__file__),'indexer/map' )
        shutil.rmtree(self.index_map, ignore_errors=True)
        pathlib.Path(self.index_map).mkdir(parents=True, exist_ok=True)

        # Create directory for reduce job and inverted index
        self.index_reduce = os.path.join(os.path.dirname(__file__),'indexer/index' )
        shutil.rmtree(self.index_reduce, ignore_errors=True)
        pathlib.Path(self.index_reduce).mkdir(parents=True, exist_ok=True)

    def start(self):
        nodes = []

        # Setup index directory for map reduce task
        self.setup_index_dir()

        # Create Master Node and start process
        master = IndexMasterNode(self.master_addr[0], self.master_addr[1] , self.worker_num, self.index_dir)
        nodes.append(master)
        master.start()

        # Load addresses for worker nodes
        addr_list = []
        with open(os.path.join(os.path.dirname(__file__), 'hosts.txt'), 'r') as f:
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
            node = IndexWorkerNode(host, port, self.master_addr, self.index_map, self.index_reduce)
            nodes.append(node)
            node.start()

        # Set worker addresses
        self.nodes['workers'] = worker_addr

        for node in nodes:
            node.join()


class IndexClient:
    def __init__(self, index_dir, num_nodes, host='localhost', port=9803):
        self.num_nodes = num_nodes
        self.index_dir = index_dir
        self.master_host = host
        self.master_port = int(port)

    def start(self):
        ip = (self.master_host, self.master_port)
        inx = IndexCluster(ip, self.num_nodes, self.index_dir)
        inx.start()


if __name__ == "__main__":
   input_dir = os.path.join(os.path.dirname(os.path.abspath(__name__)),'inputs')
   inx = IndexClient(input_dir, 2, host='localhost', port=9859)
   inx.start()
