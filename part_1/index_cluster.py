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
import pdb
import time
import threading 
from multiprocessing import Process, JoinableQueue, current_process, Lock
from base import MasterNode, WorkerNode, MessageBuilder, MessageParser, logger
from map_reduce import MapReduceProcess


class IndexWorkerNode(WorkerNode):
    # TASK
    FREE = 9  # Available to start another task
    MAP = 10
    REDUCE = 11

    # Task States
    COMPLETED = 5
    NOT_COMPLETED = 6

    queueOfJobs = queue.Queue()

    def __init__(self, host, port, worker_num, map_dir, red_dir):
        self.map_dir = map_dir
        self.red_dir = red_dir
        self.task_queue = []
        self.map_task_queue = []
        super(IndexWorkerNode, self).__init__(host, port, worker_num)

    def start_worker(self):
        self.curr_task = self.FREE
        self.task_status = self.NOT_COMPLETED

        self.map_queue_handler = threading.Thread(target=self.map_queue_handler)
        self.map_queue_handler.start()

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
    
    def map_queue_handler(self):
        self.mapper_free = True
        while True:
            try:
                if len(self.map_task_queue) > 0 and self.mapper_free:
                    self.mapper_free = False
                    task_obj = self.map_task_queue[-1]
                    self.map_task_queue.remove(task_obj)
                    self.map_task(task_obj)
                    self.mapper_free = True
            except:
                break

    def map_task(self, task_obj):
        import re
        import nltk
        from nltk.corpus import stopwords
        from nltk.tokenize import word_tokenize
        stop_words = set(stopwords.words('english'))
        dir_path = os.path.abspath(os.path.join(
            os.path.dirname(os.path.abspath(__name__)), '../inputs'))
        input_file_name = task_obj.get("dir").split("/")[-1:][0]
        begin = int(task_obj.get("start"))
        end = int(task_obj.get("end"))
        indexer_map_dir_path = os.path.join(os.path.dirname(
            os.path.abspath(__name__)), os.path.join('indexer','map'))
        raw_input_file_name = input_file_name.split(".")[0]
        if not os.path.exists(os.path.abspath(os.path.join(indexer_map_dir_path, raw_input_file_name))):
            lock = Lock()
            lock.acquire()
            os.mkdir(os.path.join(indexer_map_dir_path, raw_input_file_name))
            lock.release()
        input_file_dir = os.path.join(os.path.abspath(indexer_map_dir_path), raw_input_file_name)
        input_mapper_file = os.path.join(input_file_dir, input_file_name + "&Mapper" + str(self.port) + '.txt')
        myMapFile = open(os.path.abspath(input_mapper_file), "w+")
        finalArray = []
        fp = open(task_obj.get("dir"),'r')
        for i,line in enumerate(fp):
            if i>= begin and i < end:
                # Remove special characters characters from the line: , . : ; ... and numbers
                # add 0-9 to re to keep numbers 
                # Make all words to lower case
                line = re.sub('[^A-Za-z]+', ' ', line).lower()
                # Tokenize the words into vector of words
                word_tokens = word_tokenize(line)
                # Remove non-stop words
                [finalArray.append(word) for word in word_tokens if word not in stop_words]
        finalArray.sort()
        for i in finalArray:
            myMapFile.write(i + "," + str(1) + "\n")
        myMapFile.close()
        fp.close()

    def handle_map_task(self, message):
        directory = message.map_dir
        start = message.map_range_start
        end = message.map_range_end
        task_obj = {
            'task': 'map',
            'dir': directory,
            'start': start,
            'end': end
        }
        # self.queueOfJobs.put(task_obj)
        # self.task_status = self.NOT_COMPLETED
        # self.curr_task = self.MAP
        # while not self.queueOfJobs.empty():

        #self.map_task(task_obj)
        
        #Set this mapper to Free status
        # self.task_status = self.FREE
        # self.curr_task = self.FREE
        self.map_task_queue.append(task_obj)

    def handle_reduce_task(self, message):
        directory = message.red_dir
        start = message.red_start_letter
        end = message.red_end_letter
        task_obj = {
            'task': 'reduce',
            'dir': directory,
            'start': start,
            'end': end
        }
        self.reduce_task(task_obj)
    
    def reduce_task(self,task_obj):
        self.task_queue.append(task_obj)

        

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
        super(IndexMasterNode, self).__init__(host, port, worker_num)

    def start_master(self):
        # Handles iterating through job stages
        self.curr_job = self.PARTITION  # Partition or Map or Reduce
        self.job_status = self.NOT_STARTED
        self.index_status = self.RUNNING

        # Directory where partitions are stored
        self.storage_dir = '\partions'

        # logger.info('Starting Indexing Job')

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
                # logger.info('Partitioning Index Files For Worker Nodes')
                self.job_status = self.IN_PROGRESS
                arrayOfFilesAndSize = self.populateArrayOfFilesAndSize(
                    self.index_dir)
                # is it better to read then distribute the blocks, or distribute while reading?
                # We should read then distribute
                self.distributeJobToMappers(arrayOfFilesAndSize)
                #Master Node now has to call reducers
                # indexer/map/
                self.distributeJobToReducers(os.path.join(os.path.dirname(__file__),'indexer/map'))

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
            arrayOfFilesAndSize = np.append(
                arrayOfFilesAndSize, [{pathToFile: lines}])
        return arrayOfFilesAndSize

    def distributeJobToMappers(self, arrayOfDictionariesOfFilesPaths):
        """
            Function receives an array of dictionaries where each dictionary has the form:
            'path/to/file': int number_of_lines}
            The function then distributes each block to each Mapper
        """
        worker_keys = list(self.worker_conns.keys())
        builder = MessageBuilder(messages=[])
        for i in arrayOfDictionariesOfFilesPaths:
            k = i
            fi = list(k.keys())[0]
            k = list(k.values())[0]
            lines = k
            t = math.ceil(k / self.num_workers)
            worker, y = 0, 0
            while (lines - t) > 0:
                k = y
                y += t
                builder.add_task_map_message(self.host, self.port, fi, k, y)
                message = builder.build()
                worker_conn = self.worker_conns[worker_keys[worker]]
                worker_conn.send(message.outb)
                lines -= t
                worker += 1
            builder.add_task_map_message(
                self.host, self.port, fi, y, (list(i.values())[0]))
            message = builder.build()
            worker_conn = self.worker_conns[worker_keys[worker]]
            worker_conn.send(message.outb)
            break

    def distributeJobToReducers(self,path):
        files = os.listdir(path)
        builder = MessageBuilder(messages=[])
        letterPerWorker = math.ceil((26/self.num_workers))
        worker_keys = list(self.worker_conns.keys())
        dire = os.path.join(os.path.dirname(os.path.abspath(__name__)), 'indexer/map/')
        for directory in files:
            filesDirectory = os.listdir(os.path.join(path,directory))
            for iFile in filesDirectory:
                y = 0
                worker, letters = 0, 26
                pathToSend = os.path.join(dire,directory + '/' + iFile)
                #print(pathToSend)
                while (letters - letterPerWorker) > 0:
                    k = y
                    y += letterPerWorker
                    builder.add_task_reduce_message(self.host, self.port, pathToSend, k, y)
                    message = builder.build()
                    builder.clear()
                    #print("sending to worker", worker)
                    #print(message.outb)
                    worker_conn = self.worker_conns[worker_keys[worker]]
                    worker_conn.send(message.outb)
                    letters -= letterPerWorker
                    worker += 1
                builder.add_task_reduce_message(self.host, self.port, pathToSend, y, 26)
                message = builder.build()
                worker_conn = self.worker_conns[worker_keys[worker]]
                worker_conn.send(message.outb)

        
            


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
        self.index_map = os.path.join(os.path.dirname(__file__), 'indexer/map')
        # shutil.rmtree(self.index_map, ignore_errors=True)
        if not os.path.exists(self.index_map):
            pathlib.Path(self.index_map).mkdir(parents=True, exist_ok=True)

        # Create directory for reduce job and inverted index
        self.index_reduce = os.path.join(
            os.path.dirname(__file__), 'indexer/reducers')
        # shutil.rmtree(self.index_reduce, ignore_errors=True)
        if not os.path.exists(self.index_reduce):
            pathlib.Path(self.index_reduce).mkdir(parents=True, exist_ok=True)

    def start(self):
        nodes = []

        # Setup index directory for map reduce task
        self.setup_index_dir()

        # Create Master Node and start process
        master = IndexMasterNode(self.master_addr[0], self.master_addr[
                                 1], self.worker_num, self.index_dir)
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
                    if res != 0:  # Port is not in use by another process
                        addr_list.append(addr)  # add as available

        # Start worker nodes and append to list
        worker_addr = []
        for i in range(self.worker_num):
            addr = addr_list[i]
            worker_addr.append(addr)
            host = addr[0]
            port = int(addr[1])
            node = IndexWorkerNode(
                host, port, self.master_addr, self.index_map, self.index_reduce)
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
    input_dir = os.path.join(os.path.dirname(
        os.path.abspath(__name__)), 'inputs')
    inx = IndexClient(input_dir, 2, host='localhost', port=9859)
    inx.start()
