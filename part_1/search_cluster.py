import asyncio
import os
import pathlib
import shutil
import selectors
import socket
import sys
import numpy as np
import math
import string
import threading
import time
from multiprocessing import Process, JoinableQueue as Queue, current_process
from base import MasterNode, WorkerNode, MessageBuilder, MessageParser, create_logger

logger = create_logger()

class SearchWorkerNode(WorkerNode):
    # OUTCOME
    FOUND = 3
    NOT_FOUND = 4

    # Task States
    COMPLETED = 5
    NOT_COMPLETED = 6
    def __init__(self, host, port, master_addr):
        super(SearchWorkerNode, self).__init__(host, port, master_addr)
        self.search_tasks = [] # list of search requests that needs to be fulfilled
        self.curr_task = {} # current task that is being fullfilled
        

    def start_worker(self):
        self.task_status = self.NOT_COMPLETED
        worker_task_thread = threading.Thread(target=self.handle_tasks)
        worker_task_thread.start()
        logger.info('Worker Node started at: %s'%str(self.master_addr))
    
    def handle_request(self, conn, addr, received):

        # Parse Message
        received = received.decode("utf-8")
        parser = MessageParser()
        parsed = parser.parse(received)

        # Get keyword list of words
        if parsed.keywords:
            self.handle_keyword(parsed)

    def handle_tasks(self):
        print('handling tasks')
        # check if any tasks needed to be completed

    def handle_keyword(self, parsed):
        logger.info('Searching directory for %s '%parsed.keywords)
        # Get keyword and search directory for keyword (could deploy small tasks)
        # Get documents with kw and frequency
        # append to result array
        # send master node result

    def fetch_keyword_data(self,keyword, file):
        pass
class SearchMasterNode(MasterNode):
    def __init__(self, host, port, worker_num, index):

        self.index_dir = index
        # map of clients connections who are awaiting a response
        self.waiting_clients = {}
        # self.task_queue = Queue()
        # self.partial_task = Queue()
        # self.rank_queue = Queue()
        self.task_queue = []
        self.partial_task = []
        self.rank_queue = []
        self.task_count = 0 # will be used for id
        # Keep tabs on the number of task each worker is responsible for
        # key => port, value => { key => job_id, value => keyword list }
        self.worker_assignments = {}
        self.continue_to_next_task = True
        super(SearchMasterNode, self).__init__(host, port, worker_num)

    def start_master(self):
        logger.info('Starting Search Query Master')
        # Run a concurrent thread to handle ranking jobs
        master_job_thread = threading.Thread(target=self.handle_task_jobs)
        master_job_thread.start()
        master_rank_thread = threading.Thread(target=self.handle_ranking_jobs)
        master_rank_thread.start()

    def handle_failed_worker(self, conn, data, worker):
        logger.info('Worker %s failed attempting to restart' %str(worker))
        # Reinstantiate worker
        new_worker = SearchWorkerNode(worker[0], worker[1], self.master_addr)
        new_worker.start()

        # TODO: Reassign tasks that failed


    def handle_request(self, conn, addr, received):
        # Call this first to make sure worker nodes are being added to list
        super().handle_request(conn, addr, received)

        # Parse the message received from sender
        received = received.decode("utf-8")
        parser = MessageParser()
        parsed = parser.parse(received)

        if parsed.type == 'client':
            self.handle_client_message(parsed, conn)
        if parsed.type == 'worker':
            self.handle_worker_message(parsed, conn)


    def handle_client_message(self, parsed, conn):
        if len(parsed.keywords) > 0:
            task = {
                'keywords': parsed.keywords,
                'conn': conn,
                'task_id': self.task_count
            }
            # add a new task to the queue to be sent to the worker
            #self.task_queue.put_nowait(task)
            self.task_queue.append(task)
            self.task_count += 1
            logger.info('adding keyword search query to task queue: '+str(task)+'')
        else:
            # Send an immediate empty results if no keywords were sent
            builder = MessageBuilder(messages=[])
            builder.add_client_response_message(self.host, self.port, str({}))
            message = builder.build()
            try:
                conn.send(message.outb)
            except:
                logger.info('Failed to send response to client %s', parsed.clientid)

    def handle_worker_message(self, parsed, conn):
        if parsed.status == 'completed':
            results = parsed.results
            #  get task from partial task queue based on task_id
            task_id = results['task_id']
            hits = results['hits']
            if task_id in self.partial_task:
                idx = self.partial_task.index(task_id)
                job_num = self.partial_task[idx]['waiting_jobs']

                # add to list if there were any matches
                if len(hits) > 0:
                    self.partial_task[idx]['results'].append( hits)
                self.partial_task[idx]['waiting_jobs']-=1

                # if no more waiting_job then ready for ranking
                if self.partial_task[idx]['waiting_jobs'] == 0:
                    self.rank_queue.append(self.partial_task[idx])
                    self.partial_task.remove(task_id)

            # remove assignment from worker list
            worker_key = (parsed.host, parsed.port)
            del self.worker_assignments[worker_key][task_id]

    def handle_task_jobs(self):
        # Continously pull tasks from self.work_queue, process, and executed
        logger.info('Waiting for new task queries')
        while True:
            if self.worker_status == self.ALL_CONNECTED:
                # check if self.work_queue has tasks ready to process
                if self.continue_to_next_task and len(self.task_queue) > 0:
                    task = self.task_queue.pop()
                    print(task)
                    # Wait until we finish the process task first
                    self.continue_to_next_task = False
                    self.process_task(task)
                    self.continue_to_next_task = True

    def process_task(self, task):

        #Get task from queue

        task_id = task['task_id']
        keywords = task['keywords'].sort()
        logger.info('=====Processing task %s with keywords: %r ====='%(str(task_id), str(keywords)))
        # Get the length of keywords
        # TODO: remove stopwords
        num_keywords = len(keywords)

        # TO DO: Partition jobs
        available_workers = []
        busy_workers = []
        left_over_jobs = []

        work_distribution =[]
        max_payload = 1
        # Calculate workers-need to calculate work distribution

        # First check if any workers readily available
        for worker in self.worker_conns.keys():
            # check if worker has tasks assigned
            if worker in self.worker_assignments:
                # Compute worker payloads
                num_tasks = len(self.worker_assignments[worker].items())
                worker_load = sum([len(kw) for kw in self.worker_assignments[worker].items()
                ['keywords'] ])
                worker_max_load = max([len(kw) for kw in self.worker_assignments[worker].items()
                ['keywords'] ])
                work_distribution.append((worker, worker_load, worker_max_load, num_tasks))
                if worker_max_load > max_payload:
                    max_payload = worker_max_load
                if num_tasks == 0:
                    available_workers.append(worker)
                else:
                    busy_workers.append(worker)
            else:
                available_workers.append(worker)
        
        if len(available_workers) == 0 or num_keywords > max_payload:
            total_payload = sum([item[1] for item in work_distribution])
            worker_percentage = []
            for worker_tuple in work_distribution:
                worker = worker_tuple[0]
                load = worker_tuple[1]
                percentage = load/total_payload
                worker_percentage.append((worker, worker_percentage))
            
            # Get the number of workers need
            helpers_needed = math.floor(num_keywords/max_payload) - len(available_workers)

            if helpers_needed < len(self.worker_conns.keys()):
                # Sort the list of workers by percentage and get the worker with the lowest percentage
                worker_percentage.sort(key=lambda tup: tup[1], reverse=True)
                available_workers.append([tup[0] for tup in worker_percentage[:helpers_needed]])
            else:
                available_workers = self.worker_conns.keys()




    
        # # if we have enough available workers, then partion and send job
        # if len(available_workers) >= num_keywords:
        # Assign each worker with a responsible subset of keywords
        kw_partitions = keywords % len(available_workers)
        task['results'] = []
        task['waiting_jobs'] = kw_partitions
        for i in range(kw_partitions):
            start = i * kw_partitions
            end = ((i+1) * kw_partitions)-1
            kw_split = keywords[start:end]
            kw_job = ','.join(kw_split)

            # create a job message
            builder = MessageBuilder(messages=[])
            builder.add_task_search_message(self.host, self.port, kw_job)
            message = builder.build()
            builder.clear()

            try:
                conn.send(message.outb)
                logger.info('sent search help job: %s'%str(kw_job))
                # get worker and update assignments
                worker = available_workers[i]
                conn = self.worker_conns[worker]
                if worker in self.worker_assignments:
                    self.worker_assignments[worker][task_id] = task
                else:
                    self.worker_assignments[worker] = {}
                    self.worker_assignments[worker][task_id] = kw_job
            except:
                logger.info('failed to send job %s to worker %s'%(kw_job, worker))
                left_over_jobs.append(kw_job)

        # will process task once all jobs has been fulfilled
        self.partial_task[task_id] = task

    def handle_ranking_jobs(self):
        logger.info('Waiting for new rank jobs')
        i = 0
        # Continously pull tasks from self.rank_queue, process, and send back to client
        while True:
            i =+ 1


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
        try:
            logger.info('Starting search query master cluster')
            master = SearchMasterNode(self.master_addr[0], self.master_addr[1] , self.worker_num, self.index_dir)
            nodes.append(master)
            master.start()
        except:
            raise ConnectionError('Error starting master node and search query master cluster, please try again or use a different port')
            exit(1)

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
        time.sleep(2)
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
