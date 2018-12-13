import argparse
import socket
import sys
from search_cluster import SearchWorkerNode
from os import path
from pathlib import Path

'''
    Search Worker.py
    Usage: python search_worker.py -port [worker_port] -m_host [master_host] -m_port [master_port]

    -port: {optional} port address for the master server, will also be used for the worker nodes
    -m_host: {optional} host address for the master server
    -m_port: {optional} host port for the master server
'''

HOST = ''
M_HOST = 'localhost'
M_PORT = 9878
PORT = None

def set_port(parser, args):
    ''' Check to see if port is available '''
    global HOST, PORT
    if args.port is not None:
        port = int(args.port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            res = sock.connect_ex((HOST, port))
            if res != 0:
                PORT = port
            else:
                parser.error("Port is not available for use, please enter a different port number")
    else:
        # Load addresses for worker nodes
        addr_list = []
        with open(path.join(path.dirname(__file__), 'search_hosts.txt'), 'r') as f:
            for line in f:
                l = line.strip()
                port = int(l)
                addr = (HOST, port)
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    res = sock.connect_ex(addr)
                    if res != 0: # Port is not in use by another process
                        PORT = port
                        break


def set_master_host(parser, args):
    ''' Check to see if valid host '''
    global M_HOST
    if args.m_host is not None:
        host = args.m_host
        try:
            socket.gethostbyname(host)
            M_HOST = host
        except:
            parser.error("Invalid host, please enter a valid host")

def set_master_port(parser, args):
    ''' Check to see if master is available '''
    global HOST, M_PORT
    if args.m_port is not None:
       M_PORT = int(args.m_port)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        res = sock.connect_ex((M_HOST, int(M_PORT)))
        if res != 0:
            parser.error("Master is unreachable, please try again!")

def main():
    # Run Search Master Node if all configuration is fine
    try:
        worker = SearchWorkerNode(HOST, PORT, (M_HOST, M_PORT) )
        worker.daemon = True
        worker.start()
        worker.join()
    except Exception as e:
        err = ConnectionError('Worker Node failed, please restart')
        sys.exit(e)
    return


if __name__ == "__main__":

    # Arguments Configuration
    parser = argparse.ArgumentParser(prog="Mini Google Search", description="A mini google distributed search system ")
    parser.add_argument("-m_port", dest="m_port", action="store", type=str, help="Master Search Node Host for search system" )
    parser.add_argument("-m_host", dest="m_host", action="store", type=str, help="Master Search Node Host for search system" )
    parser.add_argument("-port", dest="port", action="store",  type=int, help="Port for worker node")

    # Argument Parser
    args = parser.parse_args()
    set_port(parser, args)
    set_master_host(parser, args)
    set_master_port(parser, args)

    # Start if all input is valid and set
    main()

