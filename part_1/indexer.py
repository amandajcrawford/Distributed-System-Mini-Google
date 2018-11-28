import logging
import argparse
from cluster import IndexCluster
from os import path

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)

INDEX_DIR = path.join('__file__', '/input')
NUM_NODES = 4

def set_index_dir(parser, args):
    if args.dir is not None:
        dir = args.dir
        if not path.isdir(dir):
            parser.error("Directory not found")
        else:
            INDEX_DIR = dir

def set_num_cluster_nodes(parser, args):
    if args.nodes is not None:
        nodes = args.nodes
        if nodes < 2:
            parser.error('Cluster should have more than 1 node')
        else:
            NUM_NODES = nodes
    return

def main():
    # Get Cluster Node Count
        # Setup Servers For Index and Search
    index_sys = IndexCluster(NUM_NODES, INDEX_DIR)
    index_sys.start()
    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Mini Google Indexer", description="A mini google distributed map reduce indexing system ")
    parser.add_argument("-fs", dest="dir", action="store", type=str, help="Enter the full path to the directory to the set of documents to be documented")
    parser.add_argument("-nodes", dest="nodes", action="store", type=int, help="Maximum number of nodes in indexing cluster")
    args = parser.parse_args()
    set_index_dir(parser, args)
    set_num_cluster_nodes(parser, args)

    main()


