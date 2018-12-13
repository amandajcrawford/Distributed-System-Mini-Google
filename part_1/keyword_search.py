''''
    Test Client for Part 1 Search System

    required arguments
    * search master host 
    * search master port 
    * -- key_words list of keywords to query

'''

import socket

import time
import random
import argparse
import threading
from time import sleep
from base import MessageBuilder, MessageParser

def main():
    arguments = get_parser().parse_args()
    kw = list(arguments.kw)
    print(kw)
    m_port = int(arguments.m_port)
    m_host = arguments.m_host 
    search_addr = (m_host, m_port)

    # Query Search Worker
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect_ex(search_addr)
    kws = ','.join(kw)
    builder = MessageBuilder()
    builder.add_keyword_search_message(1,'', 0, kws)
    msg = builder.build()

    # Measure the time it takes to recieve a response from the search system
    start = time.time()
    conn.send(msg.outb)
    data = conn.recv(1024)
    end = time.time()
    print("*****Search System Response Time:",end - start, " ********")
    if data:
        data = data.decode("utf-8")
        parser = MessageParser()
        parsed = parser.parse(data)
        results = parsed.results
        order = 1
        #print out documents
        for doc in results:
            print("Rank #", order," : ", doc[0], ", Computed TF/IDF:",doc[1])
            order+=1
def get_parser():
    parser = argparse.ArgumentParser(description='Search Tester Clients')
    parser.add_argument('m_host', type=str)
    parser.add_argument('m_port', type=int)
    parser.add_argument('--key_words' , dest="kw", nargs='+', required=True)
    return parser


if __name__ == "__main__":
    main()
