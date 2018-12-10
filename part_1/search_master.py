import argparse
import socket
from search_cluster import SearchClient
from os import path
from pathlib import Path
'''
    Search.py
    Usage: python search.py  -index [index_directory] -nodes [ds_nodes] -host [ds_host] -port [master_port]

    -index: {optional} directory to the inverted in, default looks at \input directory
    -num_nodes: {optional} number of worker nodes to be instantiated
    -port: {optional} port address for the master server, will also be used for the worker nodes
    -host: {optional} host address for the master server
'''
INDEX_DIR = path.join(Path(path.dirname(__name__)).parent, 'reducers')
NUM_NODES = 2
HOST = 'localhost'
PORT = 9877

def set_index_dir(parser, args):
    ''' Check if index directory is valid'''
    global INDEX_DIR
    if args.dir is not None:
        dir = args.dir
        print( path.join(Path(path.dirname(path.abspath(__name__))).parent,dir))
        if path.isdir(dir):
            INDEX_DIR = dir
        elif path.isdir(path.join(path.dirname(path.abspath(__file__)),dir)):
            INDEX_DIR = path.join(path.dirname(path.abspath(__file__)),dir)
        elif path.isdir(path.join(Path(path.dirname(path.abspath(__name__))).parent,dir)):
            INDEX_DIR = path.join(Path(path.dirname(path.abspath(__name__))).parent,dir)
        else:
            parser.error("Directory not found, please enter the absolute path")


def set_num_cluster_nodes(parser, args):
    ''' Check if valid number of nodes '''
    global NUM_NODES
    if args.nodes is not None:
        nodes = args.nodes
        if nodes < 2:
            parser.error('Cluster should have more than 1 node')
        else:
            NUM_NODES = nodes-1


def set_cluster_host(parser, args):
    ''' Check to see if valid host '''
    global HOST
    if args.host is not None:
        host = args.host
        try:
            socket.gethostbyname(host)
            HOST = host
        except:
            parser.error("Invalid host, please enter a valid host")

def set_cluster_master_port(parser, args):
    ''' Check to see if port is available '''
    global HOST, PORT
    if args.port is not None:
        port = args.port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            res = sock.connect_ex((HOST, port))
            if res != 0:
                PORT = port
            else:
                parser.error("Port is not available for use, please enter a different port number")

def main():
    search_sys = SearchClient(INDEX_DIR, NUM_NODES, host=HOST, port=PORT)
    search_sys.start()
    return

if __name__ == "__main__":

    # Arguments Configuration
    parser = argparse.ArgumentParser(prog="Mini Google Indexer", description="A mini google distributed map reduce indexing system ")
    parser.add_argument("-index", dest="dir", action="store", type=str, help="Enter the full path to the directory to the inverted index")
    parser.add_argument("-nodes", dest="nodes", action="store", type=int, help="Maximum number of nodes in indexing cluster")
    parser.add_argument("-host", dest="host", action="store", type=str, help="Host for search system" )
    parser.add_argument("-port", dest="port", action="store",  type=int, help="Port for master node")

    # Argument Parser
    args = parser.parse_args()
    set_index_dir(parser, args)
    set_num_cluster_nodes(parser, args)
    set_cluster_host(parser, args)
    set_cluster_master_port(parser, args)

    # Start if all input is valid and set
    main()


