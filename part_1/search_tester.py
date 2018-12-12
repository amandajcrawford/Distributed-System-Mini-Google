# Assumptions:
# client_id starts at 0
# clients know input file path

import socket
import select
import time
import random
import argparse
import threading
from time import sleep
from base import MessageBuilder, MessageParser

class SearchClient:
    def __init__(self, client_id, search_addr, file_name, client_count, schedule):
        self.id = client_id
        self.client_count = client_count
        self.assigned_server = search_addr
        self.input_path = file_name
        self.schedule = schedule
        self.connections = []
        self.keyword_set = []
        self.keyword_set_idx = 0
        self.get_keyword_set()
        print("Starting search tester %s" % self.id)

    def request(self):
        while len(self.keyword_set) > self.keyword_set_idx:
            connection = self.__connect(True)
            kws = self.keyword_set[self.keyword_set_idx]
            builder = MessageBuilder()
            builder.add_keyword_search_message(self.id,socket.gethostname(), 0, kws)
            msg = builder.build()
            connection.send(msg.outb)
            print('Search Client %s sent keyword search for: %s' % (self.id, msg))
            self.connections.append(connection)
            self.keyword_set_idx = self.keyword_set_idx + 1
            (read, write, exceptions) = select.select(self.connections, [], [], 1)
            for conn in read:
                data = conn.recv(1024)
                if data:
                    data = data.decode("utf-8")
                    parser = MessageParser()
                    parsed = parser.parse(data)
                    results = parsed.results
                    # for kw, info in results.items():
                    #     print(kw, info)
                    print(results)

            for err in exceptions:
                err.close()
            time.sleep(self.schedule)

    def get_keyword_set(self):
        num_file = open(self.input_path, 'r')
        lines = num_file.readlines()
        for i in range(0, len(lines)):
            if i % self.client_count == self.id:
                self.keyword_set.append(lines[i].strip())
        num_file.close()

    def __connect(self, retry):
        try:
            to_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            to_socket.connect(self.assigned_server)
            print("Search client %s connected to server %s" % (self.id, self.assigned_server))
        except socket.error:
            if retry:
                time.sleep(2)
                return self.__connect(self.assigned_server, False)
            else:
                print("Search Client %s Socket Error: %s" % (self.id, socket.gaierror.message))
        finally:
            return to_socket

    def close(self):
        self.connection.close()


random.seed(11)
def main():
    arguments = get_parser().parse_args()
    file_name = arguments.file_name
    m_port = arguments.m_port
    m_host = arguments.m_host 
    search_addr = (m_host, m_port)
    num_client = arguments.num_clients
    clients = list()
    client_threads = list()
    p_assign = 0
    cid = 1

    for w in range(num_client):
        p_assign = p_assign + 1
        schedule = random.randint(20, 60)
        searcher = SearchClient(cid, search_addr, file_name, num_client,schedule)
        clients.append(searcher)
        cid = cid + 1

    random.shuffle(clients)
    for c in clients:
        try:
            c_thread = threading.Thread(target=c.request)
            client_threads.append(c_thread)
            c_thread.start()
            sleep(5)
        except Exception as e:
            print(str(e))


def get_parser():
    parser = argparse.ArgumentParser(description='Search Tester Clients')
    parser.add_argument('file_name', type=str)
    parser.add_argument('m_host', type=str)
    parser.add_argument('m_port', type=int)
    parser.add_argument('num_clients', type=int)
    return parser


if __name__ == "__main__":
    main()
