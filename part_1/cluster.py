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
import types
import time
from multiprocessing import Manager, Process, current_process, Queue


def create_logger():
    multiprocessing.log_to_stderr()
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.DEBUG)
 
    fh = logging.FileHandler("index-%s.log"%str(time.time()), mode='w+')
    fh.setLevel(logging.DEBUG)
    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)
    fh.setFormatter(formatter)
 
    logger.addHandler(fh)
    return logger

logger = create_logger()
#Source: https://www.shanelynn.ie/using-python-threading-for-multiple-results-queue/
#https://realpython.com/python-sockets/

class ProcessNode(Process):
    def __init__(self, host, port, start_handler=None, conn_handler=None, req_handler=None, data=None):
        self.host = host
        self.port = port
        self.handle_connection = None
        self.handle_request = None
        self.start_handler = None
        if callable(conn_handler):
            self.handle_connection = conn_handler
        if callable(req_handler):
            self.handle_request = req_handler
        if callable(start_handler):
            self.start_handler = start_handler
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
        self.selector.register(conn, events, data=data)
        if self.handle_connection is not None:
            self.handle_connection(conn, addr, data)

    def service_connection(self, key, mask):
        sock = key.fileobj
        data = key.data
        if mask & selectors.EVENT_READ:
            recv_data = sock.recv(1024)  # Should be ready to read
            if recv_data:
                logger.info('received '+repr(recv_data)+ ' from connection '+ str(data.addr))
                data.outb += recv_data
                if self.handle_request is not None:
                    self.handle_request(sock, data, recv_data)
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
    def __init__(self, host, port):
        self.worker_address = {}
        self.num_workers = 0
        super(MasterNode, self).__init__(host, port)

        if callable(self.start_master):
            self.start_master()
        else:
            NotImplementedError('start_master not yet implemented')

class WorkerNode(ProcessNode):
    def __init__(self, host, port, master_addr):
        self.master_addr = master_addr
        super(WorkerNode, self).__init__(host, port)
        self.register()

    def register(self):
        sel = selectors.DefaultSelector()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        sock.connect_ex(self.master_addr)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        data = RPCMessageBuilder().add_registration_message(self.port)
        sel.register(sock, events, data=data)
        logging.info('Worker Node %s connected to master node'%str(self.port))

class IndexWorkerNode(WorkerNode):
    def start_worker(self):
        pass
class IndexMasterNode(MasterNode):
    def start_master(self):
        pass
class IndexCluster:
    Clustered = 2
    Non_Clustered = 3

    Started = 4
    Finished = 5

    def __init__(self, master_addr, worker_num):
        if not master_addr:
            self.master_addr = ("localhost", 8956)
        else:
            self.master_addr = master_addr

        if not worker_num:
            return RuntimeError("Worker nodes not defined")

        self.cluster_status = self.Non_Clustered
        self.nodes = {"master": master_addr, "workers": []}
        self.worker_num = worker_num


    def start(self):
        nodes = []

        # Create Master Node'
        master = IndexMasterNode(self.master_addr[0], self.master_addr[1] )
        nodes.append(master)
        master.start()

        addr_list = []
        with open(os.path.join(os.path.dirname(__file__), 'hosts.txt'), 'r') as f:
            for line in f:
                l = line.strip().split(' ')
                addr = tuple(l)
                addr_list.append(addr)


        worker_addr = []
        for i in range(self.worker_num):
            addr = addr_list[i]
            worker_addr.append(addr)
            host = addr[0]
            port = int(addr[1])
            node = IndexWorkerNode(host, port, self.master_addr)
            nodes.append(node)
            node.start()

        self.nodes['workers'] = worker_addr

        for node in nodes:
            node.join()


#TODO: Add Message Parser
class RPCMessageParser:
    pass
class RPCMessageBuilder:
    FORMAT = "RPC| %s | %s"
    def __init__(self, connid=0, messages=[], recv_total=0, outb="" ):
        self.connid = connid
        self.messages = messages
        self.recv_total = recv_total
        self.outb = outb

    def build(self):
        msg_total = sum(len(m) for m in self.messages)
        data = types.SimpleNamespace(
        connid=self.connid,
        msg_total= msg_total,
        recv_total=self.recv_total,
        messages=list(self.messages),
        outb= bytes(self.outb,'utf-8'),
        )
        return data

    def add_registration_message(self, port ):
        message = bytes("RPC | WORKER | CONNECT| %s"%str(port), 'utf-8')
        self.messages.append(message)
        self.connid += 1

    def clear(self):
        self.messages = []
        self.connid = 0


if __name__ == "__main__":
   inx = IndexCluster(('localhost', 9803), 2)
   inx.start()