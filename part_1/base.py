'''
    Bases.py - Holds all the basic templates for Mini Google Components

    Sources:
    https://www.shanelynn.ie/using-python-threading-for-multiple-results-queue/
    https://realpython.com/python-sockets/

'''
import atexit
import concurrent.futures
import logging
import logging.handlers
import multiprocessing
import os
import queue
import selectors
import socket
import sys
import time
import types
from multiprocessing import Manager, Process, Queue, current_process


def create_logger():
    multiprocessing.log_to_stderr()
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler("index-%s.log"%str(time.time()), mode='w+')
    fh.setLevel(logging.DEBUG)
    fmt = '%(processName)s - %(process)d	: %(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    return logger

logger = create_logger()

class ProcessNode(Process):
    def __init__(self, host, port, data=None):
        self.host = host
        self.port = port
        self.data = data
        super(ProcessNode, self).__init__()

    def run(self):
        self.selector = selectors.DefaultSelector()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.host, self.port))
        self.sock.listen()
        logger.info('listening on %s %s'%(self.host, self.port))
        self.sock.setblocking(False)
        self.selector.register(self.sock, selectors.EVENT_READ, data=self.data)
        atexit.register(self.shutdown)

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
        logger.info('accepted connection from '+str(addr))
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
            recv_data = sock.recv(1024)  # Should be ready to read
            if recv_data:
                logger.info('received '+repr(recv_data)+ ' from connection '+ str(data.addr))
                # Handle request using child method
                if hasattr(self, 'handle_request') and callable(self.handle_request):
                    data.outb = self.handle_request(sock, data, recv_data)
                else:
                    data.outb += recv_data
            else:
                logger.info('closing connection ' + str(data.addr))
                self.selector.unregister(sock)
                sock.close()
        if mask & selectors.EVENT_WRITE:
            if data.outb:
                logger.info("responding "+ repr(data.outb)+" to "+ str(data.addr))
                sent = sock.send(data.outb)  # Should be ready to write
                data.outb = data.outb[sent:]

    def shutdown(self):
        self.selector.close()
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()

class MasterNode(ProcessNode):
    # ALL WORKERS CONNECTED
    NONE_CONNECTED = 3
    ALL_CONNECTED = 4
    PARTIAL_CONNECTED = 5

    def __init__(self, host, port, num_workers):
    # def __init__(self, host, port, start_master=None, handle_request=None, handle_connection=None):
        self.worker_status = self.NONE_CONNECTED
        self.worker_conns = {}
        self.num_workers = num_workers
        if hasattr(self, 'start_master') and callable(self.start_master):
            self.start_master()
        else:
            NotImplementedError('start_master not yet implemented')

        return super(MasterNode, self).__init__(host, port)

    def handle_request(self, conn, addr, received):
        logger.info('Adding new worker node to list')

        # Parse incoming message
        received = received.decode("utf-8")
        parser = MessageParser()
        parsed = parser.parse(received)
        logger.info('Parsed Message: '+repr(received)+'')

        # Check if new client is  worker node
        if parsed.type == 'worker':
            if parsed.action == 'connect':
                worker_port = parsed.port
                worker_host = parsed.host
                worker_ip = (worker_host, worker_port)

                # if worker node, check if we need to add to self.worker_conns with conn
                if worker_ip not in self.worker_conns:
                    logger.info("Adding worker node %s to master list" %str(worker_port))
                    self.worker_conns[worker_ip] = conn
                    #logger.info("Num Workers %s"%self.worker_conns.keys())

                    #check to see if initial workers are connected
                    if len(self.worker_conns.keys()) == self.num_workers:
                        logger.info("All worker nodes are connected")
                        self.worker_status = self.ALL_CONNECTED
                    else:
                        self.worker_status = self.PARTIAL_CONNECTED
            return

class WorkerNode(ProcessNode):
    def __init__(self, host, port, master_addr):
        self.master_addr = master_addr
        self.master_conn = None
        super(WorkerNode, self).__init__(host, port)
        if hasattr(self, 'start_worker') and callable(self.start_worker):
                self.start_worker()

    def handle_start(self):
        ''' Register as a worker to the master node using self.master_addr '''
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        sock.connect_ex(self.master_addr)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE

        # creates a registration message to send to master node
        message = MessageBuilder(messages=[])
        message.add_registration_message(self.host, self.port)
        data = message.build()
        messages = data.outb

        self.selector.register(sock, events, data=data)

        # add data to be sent to master
        sock.send(messages)
        logging.info('Worker Node %s connected to master node'%str(self.port))
        self.master_conn = sock


class MessageParser:
    ''' Parse MessageBuilder messages '''
    def __init__(self):
        self.parsed = types.SimpleNamespace(
            type="",
            action="",
            port=0,
            host="",
            map_dir="",
            map_range_start=0,
            map_range_end=0,
            red_dir="",
            red_start_letter = "",
            red_end_letter = "",
            output_dir=""
        )

    def parse(self, message):
        # Object to get specific message data
        if len(message) == 0:
            return self.parsed

        # Split string based on MessageBuilder Format
        arr = message.lower().replace(" ", "").split("|")
        if len(arr) < 2:
            return self.parsed
        self.arr = arr

        #Get the sender type and parse based on type
        sender_type = arr[1]
        sender_host = arr[2]
        sender_port = int(arr[3])
        self.parsed.host = sender_host
        self.parsed.port = sender_port

        if sender_type == 'master':
            self.parsed.type = 'master'
            self.parse_master_message()
        elif sender_type == 'worker':
            self.parsed.type = 'worker'
            self.parse_worker_message()
        else:
            self.parsed.type = 'client'
            self.parse_client_message()
        return self.parsed

    def parse_master_message(self):
        ''' Master Messages should be defined here '''
        command = self.arr[4]

        if command == 'map':
            self.parsed.action = 'map'
            self.parsed.map_dir = self.arr[5]
            range = self.arr[6].split("-")
            self.parsed.map_range_start = range[0]
            self.parsed.map_range_end = range[1]
        elif command == 'reduce':
            self.parsed.action = 'reduce'
            self.parsed.dir = self.arr[5]
            range = self.arr[6].split("-")
            self.parsed.red_start_letter = range[0]
            self.parsed.red_end_letter = range[1]
        else:
            self.parsed.action = command


    def parse_worker_message(self):
        ''' Worker Messages should be defined here '''
        command = self.arr[4]
        if command == 'connect':
            self.parsed.action = 'connect'

    def parse_client_message(self):
        ''' Client Messages should be defined here '''
        command = self.arr[4]


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
        msg_total = sum(len(m) for m in self.messages)
        self.outb = b"|".join(self.messages)
        data = types.SimpleNamespace(
            addr = ''.join(self.addr) ,
            connid=self.connid,
            msg_total= msg_total,
            recv_total=self.recv_total,
            messages=list(self.messages),
            outb= self.outb
        )
        return data

    ''' INDEX WORKER NODE MESSAGES '''
    def add_registration_message(self, host, port ):
        self.addr = (host, str(port))
        # used for worker nodes to connect to master node
        message = bytes("RPC | WORKER | %s | %s | CONNECT "%(host, str(port)), 'utf-8')
        self.messages.append(message)
        self.connid += 1

    ''' INDEX MASTER NODE MESSAGES '''
    def add_task_map_message(self,host, port, path, range_start="", range_end=""):
        self.addr = (host, str(port))
        range = "%s-%s"%(str(range_start), str(range_end))
        # used for master to send a task message to worker node
        message = bytes("RPC | MASTER | %s| %s | MAP | %s | %s"%(host, str(port), path, str(range)) , 'utf-8')
        self.messages.append(message)
        self.connid += 1

    def add_task_reduce_message(self,host, port, path, range_start="", range_end=""):
        self.addr = (host, str(port))
        range = "%s-%s"%(str(range_start), str(range_end))
        # used for master to send a task message to worker node
        message = bytes("RPC | MASTER | %s| %s | REDUCE | %s | %s"%(host, str(port), path, str(range)) , 'utf-8')
        self.messages.append(message)
        self.connid += 1

    def clear(self):
        self.messages = []
        self.connid = 0
