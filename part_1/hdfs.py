#!/usr/bin/env python3
import logging 
import queue

class HDFS:
    def __init__(self, *args, **kwargs):
        self.phonebook = {}

    def NameNode(self, port):
        # Directory of files that are stored in the distributed system
        # key: Block Number 

        pass
    def DataNode(self, port):
        # self.local_data: {}
        pass

    def writeBlock(self, block_number, document_number, partion_number):
        # check for block_number, document_number, partion_number in self.phonebook (Name Node)
        # if exists - Do Nothing
        # if not exists: Return stream to the datanode where the block should be saved to (Name Node)
        # s1: public/data/hash.txt 
        pass
    
    def completeWrite(self, block_number, document_number, partion_number):
        # NameNode update the self.phonebook with the s1: public/data/hash.txt 
        pass

class HDFSNode:
    ''' NODE TYPE '''
    MASTER_NODE = 2
    DATA_NODE = 3
    
    ''' SERVER STATUS '''
    OFF  = 3
    SLEEP = 4
    ALIVE = 5

    ''' BLOCK SIZE'''
    BLOCK_SIZE = 128 

    def __init__(self, address, type, block_size=BLOCK_SIZE):
        self.address = address
        self.node_type = type
        self.block = block_size
        self.status = self.OFF
        self.logger = None
        self.setup_logger()


    def setup_logger(self):
        que = queue.Queue(-1)  # no limit on size
        # queue_handler = logging.handlers.QueueHandler(que)
        handler = logging.StreamHandler()
        # listener = logging.handlers.QueueListener(que, handler)
        logger = logging.getLogger('hdfs_application')
        logger.setLevel(logging.INFO)
        fh = logging.FileHandler('hdfs.log')
        # listener.start()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        # logger.addHandler(queue_handler)
        self.logger = logger
        
    def start(self):
        self.status = self.ALIVE

class NameNode(HDFSNode):
    '''
        namespace - Dictionary of documents stored amongst data_nodes in HDFS 
         Keys: Document_Title (String)
         Value: {
            # of Blocks: (Int)
            Number representing Block (Int): (DataNodeServer, File Location) (Tuple) 
         }

         data_nodes: - Dictionary of data node information
         Keys: IP/Port Number (String)
         Value: {
             address: (Int),
             size: (Int),
             free_space: (Int),
             used_space: (Int),
         }
    '''
    __DEFAULT_SIZE = 500000000

    def __init__(self, address):
        HDFSNode.__init__(self, address, self.MASTER_NODE)        
        self.namespace = {}
        self.data_nodes = {} 

    ''' Data Node Management  '''
    def add_data_node(self, address, size=__DEFAULT_SIZE):
        self.logger.info('Adding %s:%s data node to hdfs cluster', address[0], str(address[1]))
        if address not in self.data_nodes:
            self.data_nodes[address] = {
                'address': address,
                'size': size,
                'free_space': size,
                'used_space': 0
            }
    
    def remove_data_node(self, address):
        if address in self.data_nodes.keys():
            del self.data_nodes[address]
    
    ''' HDFS Operations '''
    def write_operation(self, document, block_number, block):
        pass

    def read_operation(self, document, block_number, block):
        pass

class DataNode:
    ROOT = None 
    def __init__(self, address):
        self.address = address
        self.data_dir = self.create_data_directory()
    
    def create_data_directory(self):
        return 1

    def read_block(self, block):
        pass

    def write_block(self, block):
        pass
    
    def send_success(self, block):
        pass
    
    def send_fail(self, block):
        pass

    
