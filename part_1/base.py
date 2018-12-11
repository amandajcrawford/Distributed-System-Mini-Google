'''
    Bases.py - Holds all the basic templates for Mini Google Components

    Sources:
    https://www.shanelynn.ie/using-python-threading-for-multiple-results-queue/
    https://realpython.com/python-sockets/

'''
import atexit
import asyncore
import logging
import logging.handlers
import multiprocessing
import os
import queue
import selectors
import socket
import sys
import time
import json
from datetime import datetime
import types
import psutil
from multiprocessing import Process, current_process

logger = None
def create_logger():
    global logger
    if logger is None:

        multiprocessing.log_to_stderr()
        logger = multiprocessing.get_logger()
        logger.setLevel(logging.INFO)
        now = datetime.now()
        fh = logging.FileHandler("MiniGoogle-%s.log"%now.strftime("%Y-%m-%d"), mode='w+')
        fh.setLevel(logging.INFO)
        fmt = '%(processName)s - %(process)d	: %(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(fmt)
        fh.setFormatter(formatter)

        logger.addHandler(fh)
        # atexit.register(fh.close)
    return logger
create_logger()

class ProcessNode(Process):
    global logger
    def __init__(self, host, port, data=None):
        super(ProcessNode, self).__init__()
        self.host = host
        self.port = port
        self.data = data


    def run(self):
        try:
            self.selector = selectors.DefaultSelector()
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.bind((self.host, self.port))
            self.sock.listen()
        except Exception as e:
            raise ConnectionError('Failed to connect to master node')
        finally:
            logger.info('listening on %s %s'%(self.host, self.port))
            self.sock.setblocking(False)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.selector.register(self.sock, selectors.EVENT_READ, data=self.data)
            # atexit.register(self.shutdown)
            self.host = socket.gethostname()
            # Call child start function for any pre server interaction
            if hasattr(self, 'handle_start') and callable(self.handle_start):
                self.handle_start()

            while True:
                events = self.selector.select(timeout=None)
                for key, mask in events:
                    if key.data is None:
                        self.accept_wrapper(key.fileobj)
                    else:
                        self.service_connection(key, mask)

    def accept_wrapper(self, sock):
        conn, addr = sock.accept()  # Should be ready to read
        # logger.info('accepted connection from '+str(addr))
        conn.setblocking(False)
        data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
        events = selectors.EVENT_READ | selectors.EVENT_WRITE

        # Handle connection using child function
        if hasattr(self, 'handle_connection') and callable(self.handle_connection):
            data = self.handle_connection(conn, addr, data)
        self.selector.register(conn, events, data=data)

    def service_connection(self, key, mask):
        sock = key.fileobj
        data = key.data
        if mask & selectors.EVENT_READ:
            recv_data = sock.recv(4098)  # Should be ready to read
            if recv_data:
                logger.info('received '+repr(recv_data)+ ' from connection '+ str(data.addr))
                # Handle request using child method
                if hasattr(self, 'handle_request') and callable(self.handle_request):
                    data.outb = self.handle_request(sock, data, recv_data)
                else:
                    data.outb += recv_data
            else:
                # logger.info('closing connection ' + str(data.addr))
                self.selector.unregister(sock)
                if hasattr(self, 'handle_disconnect') and callable(self.handle_disconnect):
                    self.handle_disconnect(data, sock)
                sock.close()
        if mask & selectors.EVENT_WRITE:
            if data.outb:
                # logger.info("sending "+ repr(data.outb)+" to "+ str(data.addr))
                sent = sock.send(data.outb)  # Should be ready to write
                data.outb = data.outb[sent:]
                #print(data.outb)

    def shutdown(self):
        self.selector.close()
        self.sock.close()


class MasterNode(ProcessNode):
    global logger
    # ALL WORKERS CONNECTED
    NONE_CONNECTED = 3
    ALL_CONNECTED = 4
    PARTIAL_CONNECTED = 5

    def __init__(self, host, port, num_workers):
        super(MasterNode, self).__init__(host, port)
        self.worker_status = self.NONE_CONNECTED
        self.worker_conns = {}
        self.num_workers = num_workers

    def handle_start(self):
        if hasattr(self, 'start_master') and callable(self.start_master):
            self.start_master()
        else:
            NotImplementedError('start_master not yet implemented')

    def handle_request(self, conn, addr, received):
        # Parse incoming message
        received = received.decode("utf-8")
        parser = MessageParser()
        parsed = parser.parse(received)
        # logger.info('Parsed Message: '+repr(parsed)+'')

        # Check if new client is  worker node
        if parsed.type == 'worker':
            if parsed.action == 'connect':
                # logger.info('Adding new worker node to list')

                worker_port = parsed.port
                worker_host = parsed.host
                worker_ip = (worker_host, worker_port)

                # if worker node, check if we need to add to self.worker_conns with conn
                if worker_ip not in self.worker_conns:
                    # logger.info("Adding worker node %s to master list" %str(worker_port))
                    self.worker_conns[worker_ip] = conn
                    #logger.info("Num Workers %s"%self.worker_conns.keys())

                    #check to see if initial workers are connected
                    if len(self.worker_conns.keys()) == self.num_workers:
                        # logger.info("All worker nodes are connected")
                        self.worker_status = self.ALL_CONNECTED
                    else:
                        self.worker_status = self.PARTIAL_CONNECTED
        return

    def handle_disconnect(self, conn, data):
        # try to find the worker that was disconnected
        for worker, w_conn in self.worker_conns.items():
            # delete worker from list
            if conn.addr == w_conn:
                # check if worker is still alive
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    res = sock.connect_ex(worker)
                    if res != 0:
                        # worker failed
                        # remove from worker_conn
                        del self.worker_conns
                        [worker]
                        self.num_workers -= 1

                        #call child method handler
                        if hasattr(self, 'handle_failed_worker') and callable(self.handle_failed_worker):
                            self.handle_failed_worker(conn, data, worker)
                    else:
                        self.worker[worker] = res

class WorkerNode(ProcessNode):
    def __init__(self, host, port, master_addr):
        super(WorkerNode, self).__init__(host, port)
        self.master_addr = master_addr
        self.master_conn = None

    def handle_start(self):
        ''' Register as a worker to the master node using self.master_addr '''
        if hasattr(self, 'start_worker') and callable(self.start_worker):
            self.start_worker()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        resp = sock.connect_ex(self.master_addr)
        #print(resp)
        if resp == 0:
            # logger.info('Worker Node %s failed to connect to master node'%str(self.port))
            raise ChildProcessError('Worker %s not able to connect to master' %self.port)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        # logger.info('Worker Node %s connecting to master node'%str(self.port))
        # creates a registration message to send to master node
        message = MessageBuilder(messages=[])
        message.add_registration_message(self.host, self.port)
        data = message.build()
        messages = data.outb

        self.selector.register(sock, events, data=data)

        # add data to be sent to master
        try:
            sock.send(messages)
            # logging.info('Worker Node %s connected to master node'%str(self.port))
            self.master_conn = sock
        except:
            # retry before failing
            time.sleep(3)
            sock.send(messages)
            # logging.info('Worker Node %s failed to connect to master node'%str(self.port))
            self.master_conn = sock
            # Master must have fatally failed
            exit(1)

    def handle_disconnect(self, conn, data):
        # master can reconnect again or should I shutdown?
        pass


class MessageParser:
    ''' Parse MessageBuilder messages '''
    def __init__(self):
        self.parsed = types.SimpleNamespace(
            type="",
            action="",
            port=0,
            host="",

            # Index Related Keywords
            map_dir="",
            map_range_start=0,
            map_range_end=0,
            red_dir="",
            red_start_letter = "",
            red_end_letter = "",
            output_dir="",

            # Search Related Keywords
            keywords = [],
            status = "",
            results={},
            taskid=None,
            assignment=[],
            index_dir='',

            #Server Info
            cpu = None,
            mem = None,

            # Only if data was unparsable
            unparsed = [],
            clientid = None
        )

    def parse(self, message):
        # Object to get specific message data
        if len(message) == 0:
            return self.parsed

        # Split string based on MessageBuilder Format
        
        arr = message.lower().replace(" ", "").split("|")
        self.aux = message.replace(" ","").split("|")
        if len(arr) < 2:
            return self.parsed
        self.arr = arr

        #Get the sender type and parse based on type
        sender_type = self.arr[1]
        sender_host = self.arr[2]
        sender_port = int(arr[3])
        self.parsed.host = sender_host
        self.parsed.port = sender_port

        # Get sender sys info
        self.parsed.cpu= self.arr[-1]
        self.parsed.mem = self.arr[-2]

        if sender_type == 'master':
            self.parsed.type = 'master'
            self.parse_master_message()
        elif sender_type == 'worker':
            self.parsed.type = 'worker'
            self.parse_worker_message()
        elif sender_type == 'client':
            self.parsed.type = 'client'
            self.parse_client_message()
        else:
            self.parsed.unparsed = self.arr
        return self.parsed

    def parse_master_message(self):
        ''' Messages sent from the master node should be defined here '''
        command = self.arr[4]

        if command == 'map':
            self.parsed.action = 'map'
            self.parsed.map_dir = self.aux[5]
            rangeLines = self.arr[6].split("-")
            self.parsed.map_range_start = rangeLines[0]
            self.parsed.map_range_end = rangeLines[1]
        elif command == 'reduce':
            self.parsed.action = 'reduce'
            self.parsed.red_dir = self.aux[5]
            rangeLines = self.arr[6].split("-")
            self.parsed.red_start_letter = rangeLines[0]
            self.parsed.red_end_letter = rangeLines[1]
        elif command == 'search':
            self.parsed.action = 'search'
            self.parsed.taskid = self.arr[5]
            self.parsed.keywords = self.arr[6].strip().split(",")
        elif command == 'response':
            self.parsed.action = 'response'
            self.parsed.results = eval(repr(self.arr[5]))
        elif command == 'assign':
            self.parsed.action = 'assign'
            self.parsed.index_dir = self.arr[5]
            self.parsed.assignment = self.arr[6].strip().split(",")
        else:
            self.parsed.action = command
            self.parsed.unparsed = self.arr


    def parse_worker_message(self):
        ''' Messages sent from the worker node should be defined here '''
        command = self.arr[4]
        if command == 'connect':
            self.parsed.action = 'connect'
        elif command == 'search':
            self.parsed.action = 'search'
            self.parsed.status = self.arr[5]
            self.parsed.taskid = self.arr[6]
            r_str = self.arr[7].replace("'", "\"")
            self.parsed.results = json.loads(r_str)
        elif command == 'sys':
            self.parsed.action = 'sys'
        else:
            self.parsed.action = command
            self.parsed.unparsed = self.arr

    def parse_client_message(self):
        ''' Messages sent from a client should be defined here '''
        command = self.arr[4]

        if command == 'keyword':
            self.parsed.action = 'keyword'
            self.parsed.clientid = self.arr[5]
            self.parsed.keywords = self.arr[6].strip().split(',')
        else:
            self.parsed.unparsed = self.arr


class MessageBuilder:
    ''' Create formatted messages to be sent amongst nodes in system
        Format: RPC | node_type | host | port | command | **optional**
        node_type = master or worker
    '''
    def __init__(self, connid=0, messages=[], recv_total=0, outb="" ):
        self.connid = connid
        self.messages = messages
        self.recv_total = recv_total
        self.outb = outb
        self.addr = ""

    def build(self):
        # Get the system information
        cpu_info = psutil.cpu_times().system
        mem_info = psutil.virtual_memory().percent
        message = self.messages[-1].decode('utf-8')
        print(message)
        info = "%s | %s |"%(str(mem_info), str(cpu_info))
        message = message + info
        self.outb = bytes(message, "utf-8")
        data = types.SimpleNamespace(
            addr = ''.join(self.addr) ,
            connid=self.connid,
            recv_total=self.recv_total,
            messages=list(message),
            outb= self.outb
        )
        self.clear()
        return data

    ''' CLIENT KEYWORD SEARCH MESSAGES '''
    def add_keyword_search_message(self, client_id, host, port, keyword_str):
        self.addr = (host, str(port))
        # used to send search query master keyword search messages
        message = bytes("RPC | CLIENT | %s | %s | KEYWORD | %s | %s |"%(host, str(port),client_id, str(keyword_str)), 'utf-8')
        self.messages.append(message)
        self.connid += 1

    ''' ------- WORKER NODE MESSAGES ---------- '''

    def add_registration_message(self, host, port ):
        self.addr = (host, str(port))
        # used for worker nodes to connect to master node
        message = bytes("RPC | WORKER | %s | %s | CONNECT |"%(host, str(port)), 'utf-8')
        self.messages.append(message)
        self.connid += 1

    def add_task_complete_message(self, host, port, task, task_data):
        self.addr = (host, str(port))
        # used for worker nodes to send task complete status
        message = bytes("RPC | WORKER | %s | %s | %s | COMPLETE | %s |"%(host, str(port), str(task).upper(),str(task_data)), 'utf-8')
        self.messages.append(message)
        self.connid += 1

    ''' INDEX WORKER NODE MESSAGES '''


    ''' SEARCH WORKER NODE MESSAGES '''
    def add_search_complete_message(self, host, port, taskid, task_data):
        self.addr = (host, str(port))
        # used for worker nodes to send task complete status
        message = bytes("RPC | WORKER | %s | %s | SEARCH | COMPLETE | %s | %r |"%(host, str(port), str(taskid),task_data), 'utf-8')
        self.messages.append(message)
        self.connid += 1


    ''' ------- MASTER NODE MESSAGES ---------- '''

    ''' INDEX MASTER NODE MESSAGES '''
    def add_task_map_message(self,host, port, path, range_start="", range_end=""):
        self.addr = (host, str(port))
        range = "%s-%s"%(str(range_start), str(range_end))
        # used for master to send a task message to worker node
        message = bytes("RPC | MASTER | %s| %s | MAP | %s | %s |"%(host, str(port), path, str(range)) , 'utf-8')
        self.messages.append(message)
        self.connid += 1

    def add_task_reduce_message(self,host, port, path, range_start="", range_end=""):
        self.addr = (host, str(port))
        range = "%s-%s"%(str(range_start), str(range_end))
        # used for master to send a task message to worker node
        message = bytes("RPC | MASTER | %s | %s | REDUCE | %s | %s |"%(host, str(port), path, str(range)) , 'utf-8')
        self.messages.append(message)
        self.connid += 1

    ''' SEARCH MASTER NODE MESSAGES '''
    def add_keyword_assignment_message(self, host, port, index_dir, assignment):
        self.addr = (host, str(port))
        assignment = ",".join(assignment)
        # used for master to send a task message to worker node
        message = bytes("RPC | MASTER | %s | %s | ASSIGN | %s | %s |"%(host, str(port), index_dir, assignment), 'utf-8')
        self.messages.append(message)
        self.connid += 1

    def add_task_search_message(self,host, port, task_id, keywords):
        self.addr = (host, str(port))
        keywords = ','.join(keywords)
        # used for master to send a task message to worker node
        message = bytes("RPC | MASTER | %s | %s | SEARCH | %d | %s |"%(host, str(port), task_id, keywords), 'utf-8')
        self.messages.append(message)
        self.connid += 1

    def add_client_response_message(self,host, port, result):
        self.addr = (host, str(port))
        result = str(result)
        # used for master to send a task message to worker node
        message = bytes("RPC | MASTER | %s | %s | RESPONSE | %s |"%(host, str(port), result ), 'utf-8')
        self.messages.append(message)
        self.connid += 1

    def clear(self):
        #clear the message queue
        self.messages = []
        self.connid = 0

