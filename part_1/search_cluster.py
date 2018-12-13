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
import subprocess
import psutil
import operator
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
        self.curr_task = None # current task that is being fullfilled

        self.local_index_partition = {}
        self.index_ready = False
        self.index_update = False
        self.index_assignment = []
        self.index_dir = ""

        self.kw_file_list = []
    
    def start_worker(self):
        # Start Threads for kw processing
        worker_task_thread = threading.Thread(target=self.handle_tasks)
        worker_task_thread.start()
        worker_index_updater = threading.Thread(target=self.handle_index)
        worker_index_updater.start()
        self.host = socket.gethostname()
        logger.info('Search Worker Node started at: %s : %s'%(str(self.host),str(self.port)))

    def handle_request(self, conn, addr, received):

        # Parse Message
        received = received.decode("utf-8")
        parser = MessageParser()
        parsed = parser.parse(received)

        # Check if request is keyword assignment information
        if parsed.action == 'assign':
            self.index_dir = parsed.index_dir
            self.index_assignment = parsed.assignment
            self.index_update = True
            logger.info("Worker Node %s assigned kws: %s "%(str(self.port),str(self.index_assignment)))

        # Get keyword list of words
        if parsed.action == 'search':
            logger.info("Worker Node %s receieved task %s with kws: %s "%(str(self.port),str(parsed.taskid),str(parsed.keywords)))
            self.search_tasks.append(parsed)

    def handle_tasks(self):
        while True:
            # check if any tasks needed to be completed
            if len(self.search_tasks) > 0 and self.curr_task == None and self.index_ready:
                print('task ready')
                self.curr_task = self.search_tasks[-1]
                self.search_tasks.remove(self.curr_task)
                self.handle_keywords()
                self.curr_task = None

    def handle_keywords(self):
        # # Get info from task object
        kws = self.curr_task.keywords
        results = {}
        # # Loop through keywords
        for kw in kws:
            logger.info('+++++Searching directory for %s +++++++'%kw)
            # Get keyword and search directory for keyword (could deploy small tasks)
            if kw in self.local_index_partition.keys():
                results[kw] = self.local_index_partition[kw]
                logger.info('+++++++ Found Keyword Data for %s: %s ++++++++++'%(kw, str(results[kw])))
                
        # send master node result
        builder = MessageBuilder()
        builder.clear()
        builder.add_search_complete_message(self.host, self.port, self.curr_task.taskid, results)
        message = builder.build()
        self.master_conn.send(message.outb)
        builder.clear()


    def handle_index(self):
        while True:
            # Handle initial data loading of index
            if len(self.index_assignment) > 0 and not self.index_ready and self.index_update:
                self.kw_file_list = [os.path.join(self.index_dir,""+str(i)+".txt") for i in self.index_assignment  if i != " " ]

                # Load all keyword data into hashmap
                for kw_filename in self.kw_file_list:
                    # Read data from file
                    try:
                        with open(kw_filename, 'r') as kw_file:
                            for line in kw_file.readlines():
                                line = line.strip()
                                line_arr = line.split(" ")
                                kw = line_arr[0].strip()
                                documents = line_arr[1:]
                                self.local_index_partition[kw] = documents
                    except:
                        print("Could not load from file ", kw_filename)
                self.index_ready = True
                self.index_update = False


class SearchMasterNode(MasterNode):

    def __init__(self, host, port, worker_num, index):
        self.index_dir = index
        
        # Will be updated by the self.handle_index_updates
        # key: letter, value:{ file_path: "/a", file_size" 100bytes}
        self.index_system = {}
        self.index_worker_map = {}
        self.index_ready = False

        # Set of tasks currently being handled
        self.task_map = {}
        self.task_queue = []
        self.partial_task = []
        self.rank_queue = []
        self.task_count = 0 # will be used for id

        # Keep tabs on the number of task each worker is responsible for
        # key => port, value => { key => job_id, value => keyword list }
        self.worker_assignments = {}
        self.worker_sys = {}
        self.worker_index ={}
        self.continue_to_next_task = True

        self.new_worker = False
        super(SearchMasterNode, self).__init__(host, port, worker_num)


    def start_master(self):
        logger.info('Starting Search Query Master')
        # check if index dir is accurate
        if not os.path.isdir(self.index_dir):
            sys.exit("Index Directory Not Found.... Exiting")
        # Run a concurrent thread to handle ranking jobs
        master_job_thread = threading.Thread(target=self.handle_task_jobs)
        master_job_thread.start()
        master_rank_thread = threading.Thread(target=self.handle_ranking_jobs)
        master_rank_thread.start()
        master_index_watcher_thread = threading.Thread(target=self.handle_index_updates)
        master_index_watcher_thread.start()
        #self.host = socket.gethostname()



    def handle_failed_worker(self, conn, data, worker):
        logger.info('Worker %s failed attempting to restart' %str(worker))
        if worker in self.worker_assignments.keys():
            for task in self.worker_assignments[worker].keys():
                # Remove the possibility of hanging tasks/ decreases accuracy
                self.task_map[task]['results'] -= 1
            del self.worker_assignments[worker]
        if worker in self.worker_sys.keys():
            del self.worker_sys[worker]
        if worker  in self.worker_index.keys():
            del self.worker_index[worker]
        self.new_worker = True


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
        # Get worker sys information
        worker = (parsed.host, parsed.port)
        self.worker_sys[worker] = {'cpu': parsed.cpu, 'mem': parsed.mem}

        # Check to see if worker has an index partition
        if parsed.action == 'connect':
            if worker not in self.worker_index.keys():
                self.worker_index[worker] =[]
            self.new_worker = True

        if parsed.action == 'search':
            if parsed.status == 'complete':
                results = parsed.results
                #  get task from partial task queue based on task_id
                task_id = int(parsed.taskid)
                logger.info('=========> Processing Completed Keyword Retrieval from %s for task %s with: %s'%(str(parsed.port), str(task_id), str(results)))
                if task_id in self.partial_task:
                    # add to list if there were any matches
                    if len(results.items()) > 0:
                        self.task_map[task_id]['results'].append(results)
                    self.task_map[task_id]['waiting_jobs']= self.task_map[task_id]['waiting_jobs']-1

                    # if no more waiting_job then ready for ranking
                    if self.task_map[task_id]['waiting_jobs'] == 0:
                        self.rank_queue.append(task_id)
                        logger.info('=========> Task %s ready for ranking'%(str(parsed.port)))
                        self.partial_task.remove(task_id)

                # remove assignment from worker list
                worker_key = (parsed.host, parsed.port)
                del self.worker_assignments[worker_key][task_id]


    def handle_task_jobs(self):
        # Continously pull tasks from self.work_queue, process, and executed
        logger.info('Waiting for new task queries')
        while True:
            if len(self.worker_conns.keys()) > 0:
                # check if self.work_queue has tasks ready to process
                if self.continue_to_next_task and len(self.task_queue) > 0 and self.index_ready:
                    print('handling tasks')
                    # Wait until we finish the process task first
                    self.continue_to_next_task = False
                    self.process_task()
                    self.continue_to_next_task = True


    def process_task(self):
        #Get task from queue
        task = self.task_queue[-1]
        self.task_queue.remove(task)
        task_id = task['task_id']
        kwds = task['keywords']

        logger.info('=====Processing task %s with keywords: %r ====='%(str(task_id), str(kwds)))

        # Get the length of keywords
        # TODO: remove stopwords
        num_keywords = len(kwds)

        # TO DO: Partition jobs
        assignment = {}
        for kw in kwds:
            print(kw)
            first_letter = kw[0]
            assigned_worker = self.index_worker_map[first_letter]
            if assigned_worker in assignment.keys():
                assignment[assigned_worker].append(kw)
            else:
                assignment[assigned_worker] = []
                assignment[assigned_worker].append(kw)

        sub_tasks = len(assignment.keys())
        task['results'] = []
        task['waiting_jobs'] = sub_tasks
        for worker, kw_job in assignment.items():

            # create a job message
            builder = MessageBuilder(messages=[])
            builder.add_task_search_message(self.host, self.port, task_id, kw_job)
            message = builder.build()
            builder.clear()

            try:
                # get worker and update assignments
                conn = self.worker_conns[worker]
                conn.send(message.outb)
                logger.info('====> Sending job %s to worker %s'%(str(kw_job), worker))
                if worker in self.worker_assignments.keys():
                    self.worker_assignments[worker][task_id] = kw_job
                else:
                    self.worker_assignments[worker] = {}
                    self.worker_assignments[worker][task_id] = kw_job
            except:
                logger.info('====> Failed to send job %s to worker %s'%(str(kw_job), worker))
                
        # will process task once all jobs has been fulfilled
        logger.info('====> Sent jobs for task %d with %s keywords......waiting for %d tasks to complete.'%(task_id, str(kwds), sub_tasks))
        self.task_map[task_id] = task
        self.partial_task.append(int(task_id))


    def handle_ranking_jobs(self):
        logger.info('Waiting for new rank jobs')
        continue_to_next_task = True
        # Continously pull tasks from self.rank_queue, process, and send back to client
        while True:
            if len(self.rank_queue) > 0 and continue_to_next_task:
                # Wait until we finish the process task first
                continue_to_next_task = False
                self.rank_task()
                continue_to_next_task = True
    

    def rank_task(self):
        task_id = self.rank_queue[-1]
        self.rank_queue.remove(int(task_id))
        # Get task results
        data = self.task_map[task_id]
        logger.info('====> Starting ranking for jobs for task %d with %r'%(task_id, str(data['keywords']) ))

        # Flatten result list
        documents = {}
        keyword_metrics = {}
 
        for obj in data['results']:
            for kw,v in obj.items():
                v = v[0]
                document_l = v.split(',')
                for d in document_l:
                    doc_arr = d.split(':')
                    doc_name = doc_arr[0]
                    doc_freq = doc_arr[1]
                    if kw not in keyword_metrics:
                        keyword_metrics[kw] = {}
                    if doc_name not in documents.keys():
                        documents[doc_name]={}                        
                    documents[doc_name][kw] = int(doc_freq)
                    keyword_metrics[kw][doc_name] = int(doc_freq)

        # Compute totals for and idf for kw
        print(keyword_metrics)
        print(documents)
        kw_totals = {}
        kw_idf = {}
        for kw, docs in  keyword_metrics.items():
            kw_totals[kw]= sum([freq for freq in docs.values()])
            kw_idf[kw]= 1+ math.log(len(documents.keys())/len(docs.items()), 2)

        # Compute tfidf and rank
        ranks ={}
        for doc, kw_set  in documents.items():
            doc_total = 0
            for kw, tf in kw_set.items():
                idf = kw_idf[kw]
                kw_tot = tf * idf
                doc_total += kw_tot
            ranks[doc] = doc_total
        
        final_output = sorted(ranks.items(), key=lambda kv: kv[1])

        # Send ranked document to user
        conn = data['conn']

        builder = MessageBuilder()
        builder.add_client_response_message(self.host, self.port, final_output)
        message = builder.build()

        logger.info('Task %s Complete ======>> Sending Client Rank Documents for the keywords: %s Rank: %r '%(str(task_id),str(data['keywords']),final_output))
        try:
            conn.send(message.outb)
        except:
            logger.info('*********Client Failed to receive Data ***')


    def handle_index_updates(self):
        start = True
        self.__index_update = False
        self.orig_dir = self.index_dir
        self.index_changed = False
        while True:
            if not self.__index_update:
                self.index_changed = False
                new_index_dir = ""
                # Checking if index has been updated 
                up_file = pathlib.Path(os.path.join(self.orig_dir, 'updated.txt')) 
                if up_file.is_file():
                    with open(os.path.join(self.orig_dir, 'updated.txt'), 'r') as up_file:
                        # Get the first line only
                        new_index_dir = up_file.readline()
                    
                    # Check to see if new index actually exists
                    if new_index_dir and pathlib.Path(new_index_dir).is_dir() and self.index_dir != new_index_dir:
                        logger.info('New Index System Found Index: %s'%new_index_dir)
                        self.index_dir = new_index_dir
                        self.__index_update = True
                        self.index_changed = True
                if start:
                    self.__index_update = True
                    self.index_changed = True
                    start = False 

            if self.__index_update:
                new_index ={}
                logger.info('Building Index System')
                self.index_ready = False       
                # Loop through index directory
                for file in os.listdir(self.index_dir):
                    if file.endswith(".txt") and not file.startswith('updated'):
                        letter = file.split('.')[0]
                        letter_file = os.path.join(self.index_dir, file)
                        new_index[letter]= {'file':letter_file}
                        
                        #Compute the number of lines in file
                        try:
                            with open(letter_file, 'r') as f:
                                j = 1
                                for j, l in enumerate(f):
                                    j +=1
                                new_index[letter]['size']=j 
                                print("Index for ", letter, "keywords file size: ", j)
                        except:
                            print("No Keywords containing the letter ", letter)
                
                # Update index once processing has been completed
                self.index_system = new_index
                self.__index_update = False
                self.index_ready = True
            
            worker_sys = self.worker_sys.items()
            worker_index = self.worker_index.items()
            
            if (self.index_ready and len(worker_sys) > 0):
                # check to see if we have any new workers
                #new_worker = False
                # sort index by size
                index_sorted = sorted(self.index_system.items(), key=lambda kv: kv[1]['size'])
                # sort server by mem
                worked_sorted = sorted(worker_sys, key=lambda kv: kv[1]['mem'])
                # for worker, index in worker_index:
                #     if len(index) == 0 and worker is not None:
                #         new_worker = True
                
                if self.new_worker or self.index_changed:
                    # Compute partitions
                    num_ranges = math.floor(len(index_sorted)/len(worked_sorted))
                    i = 0
                    for worker in worked_sorted:
                        worker = worker[0]

                        # assign lightest to heaviest letters
                        p_begin = num_ranges* (i)
                        p_end = num_ranges* (i + 1)

                        if p_end > len(index_sorted):
                            p_end = len(index_sorted) -1
                        
                        partition = index_sorted[p_begin:p_end]
                        kw_assignment = [kv[0] for  kv in partition]
                        for k, v in partition:
                            self.index_worker_map[k] = worker

                        # send worker assignment message
                        builder = MessageBuilder()
                        builder.add_keyword_assignment_message(self.host, self.port, self.index_dir, kw_assignment)
                        message = builder.build()
                        self.worker_conns[worker].send(message.outb)
                        self.worker_index[worker] = kw_assignment
                        logger.info('Sending Worker %s keyword assignment: %s '%(str(worker), str(kw_assignment)))
                        
                        i += 1
                        self.new_worker = False
                
        


class SearchCluster:
    '''
        SearchCluster: Instantiates a search cluster that instantiates a Search Query Master that is available to
        receive client keyword requests and Search Helper Worker Nodes that are available to assist with searching for keywords at
        any moment.

        TODO:
        Takes an initial set of workers but is able to expand......????
     '''


    def __init__(self, master_addr, worker_num, index_dir):
        if not worker_num:
            raise RuntimeError("Worker nodes not defined")

        # Get the computer host
        host = ''
        # # returns output as byte string
        # returned_output = subprocess.run(["uname", "-n"],  stdout=subprocess.PIPE)
        # # using decode() function to convert byte string to string
        # print('Current Server is:', returned_output.stdout.decode("utf-8"))
        # host = returned_output.stdout.decode("utf-8").strip()

        self.master_addr = master_addr
        #self.master_addr = (host, self.master_addr[1])
        #self.host = self.master_addr[0]
        self.host = host
        self.master_port = self.master_addr[1]
        self.nodes = {"master": master_addr, "workers": []}
        self.worker_num = worker_num
        self.index_dir = index_dir

    def start(self):
        nodes = []
        # Create Master Node and start process

        try:
            logger.info('Starting search query master cluster')
            # master = SearchMasterNode(self.master_addr[0], self.master_addr[1] , self.worker_num, self.index_dir)
            master = SearchMasterNode(self.host, self.master_addr[1] , self.worker_num, self.index_dir)
            nodes.append(master)
            master.start()
        except:
            err = ConnectionError('Error starting master node and search query master cluster, please try again or use a different port')
            sys.exit(err)

        # Load addresses for worker nodes
        addr_list = []
        with open(os.path.join(os.path.dirname(__file__), 'search_hosts.txt'), 'r') as f:
            for line in f:
                l = line.strip().split(' ')
                #print(l)
                addr = (l[0], int(l[1]))
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    res = sock.connect_ex(addr)
                    if res != 0: # Port is not in use by another process
                        addr_list.append(addr) # add as available

        # Start worker nodes and append to list
        time.sleep(2)
        try:
            worker_addr = []
            for i in range(self.worker_num):
                addr = addr_list[i]
                worker_addr.append(addr)
                #host = addr[0]
                host = self.host
                port = int(addr[1])
                node = SearchWorkerNode(host, port, self.master_addr)
                node.daemon =True
                nodes.append(node)
                node.start()

            # Set worker addresses
            self.nodes['workers'] = worker_addr
            for node in nodes:
                node.join()
        except:
            exit(1)


class SearchClient:
    def __init__(self, index_dir, num_nodes, host='localhost', port=9890):
        self.num_nodes = num_nodes
        self.index_dir = index_dir
        self.master_host = host
        self.master_port = 56724

    def start(self):
        master_ip = (self.master_host, self.master_port)
        search = SearchCluster(master_ip, self.num_nodes, self.index_dir)
        search.start()


if __name__ == "__main__":
   input_dir = os.path.abspath(os.path.join(pathlib.Path(os.path.dirname(__file__)), 'reducers'))
   search = SearchClient(input_dir, 2)
   search.start()
