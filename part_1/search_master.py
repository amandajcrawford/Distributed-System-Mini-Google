import argparse
import socket
import sys
from search_cluster import SearchMasterNode
from os import path
from pathlib import Path
'''
    Search Master.py
    Usage: python search_master.py  -index [index_directory] -host [ds_host] -port [master_port]

    -index: {optional} directory to the inverted in, default looks at \input directory
    -num_nodes: {optional} number of worker nodes to be instantiated
    -port: {optional} port address for the master server, will also be used for the worker nodes
'''
INDEX_DIR = path.abspath(
    path.join(Path(path.dirname(__file__)), 'indexer/reduce'))
NUM_NODES = 1
HOST = ''
PORT = 9878


def set_index_dir(parser, args):
    ''' Check if index directory is valid'''
    global INDEX_DIR
    if args.dir is not None:
        idir = args.dir
        print(path.join(Path(path.dirname(path.abspath(__name__))).parent, idir))
        if path.isdir(idir):
            INDEX_DIR = dir
        elif path.isdir(path.join(path.dirname(path.abspath(__file__)), idir)):
            INDEX_DIR = path.abspath(
                path.join(path.dirname(path.abspath(__file__)), idir))
        elif path.isdir(path.join(Path(path.dirname(path.abspath(__name__))).parent, idir)):
            INDEX_DIR = path.abspath(
                path.join(Path(path.dirname(path.abspath(__name__))).parent, idir))
        else:
            parser.error(
                "Directory to Index System not found, please enter the absolute path")
    print("Index Root: ", INDEX_DIR)


def set_num_cluster_nodes(parser, args):
    ''' Check if valid number of nodes '''
    global NUM_NODES
    if args.nodes is not None:
        nodes = args.nodes
        if nodes < 2:
            parser.error('Cluster should have more than 1 node')
        else:
            NUM_NODES = nodes - 1


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
                parser.error(
                    "Port is not available for use, please enter a different port number")


def main():
    # Run Search Master Node if all configuration is fine
    try:
        master = SearchMasterNode(HOST, PORT, NUM_NODES, INDEX_DIR)
        master.start()
        master.join()
    except Exception as e:
        err = ConnectionError('Master Node failed, please restart')
        sys.exit(e)
    return


if __name__ == "__main__":

    # Arguments Configuration
    parser = argparse.ArgumentParser(
        prog="Mini Google Search", description="A mini google distributed search system ")
    parser.add_argument("-index", dest="dir", action="store", type=str,
                        help="Enter the full path to the directory to the inverted index")
    parser.add_argument("-port", dest="port", action="store",
                        type=int, help="Port for master node")

    # Argument Parser
    args = parser.parse_args()
    set_index_dir(parser, args)
    set_cluster_master_port(parser, args)

    # Start if all input is valid and set
    main()
