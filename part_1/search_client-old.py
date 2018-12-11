import logging
import socket
import multiprocessing
import sys
import socket
import selectors
import types
from base import MessageBuilder, MessageParser

sel = selectors.DefaultSelector()
messages = []
receieved = []
def start_connections(host, port, num_conns):
    server_addr = (host, port)
    for i in range(num_conns):
        connid = i + 1
        print("starting connection", connid, "to", server_addr)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setblocking(False)
        sock.connect_ex(server_addr)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        sel.register(sock, events, data=messages)

            


def service_connection(key, mask):
    sock = key.fileobj
    data = key.data
    if mask & selectors.EVENT_READ:
        recv_data = sock.recv(1024)  # Should be ready to read
        if recv_data:
            print("received", repr(recv_data), "from connection",host)
        else:
            print("closing connection", host)
            sel.unregister(sock)
            sock.close()
    if mask & selectors.EVENT_WRITE:
        if data:
            m = data.pop()
            print("sending", repr(m), "to connection", host)
            sock.send(m)  # Should be ready to write

            


if len(sys.argv) != 4:
    print("usage:", sys.argv[0], "<host> <port> <num_connections>")
    sys.exit(1)

host, port, num_conns = sys.argv[1:4]
keyword_list = [
    ['architecture', 'love'],
    ['conceptualization', 'hate', 'comparisons']
]
kw_string = []
# for i in range(2):
#     kw = ",".join(keyword_list[i%2])
#     builder = MessageBuilder(messages=[])
#     builder.add_keyword_search_message(i, host, port, kw )

#     messages.append(builder.build().outb)
#     builder.clear()

for kw in keyword_list:
    kw = ",".join(kw)
    kw_string.append(kw)

for i in range(int(num_conns)):
    builder = MessageBuilder(messages=[])
    builder.add_keyword_search_message(i, host, port, kw )

    messages.append(builder.build().outb)
    builder.clear()


start_connections(host, int(port), int(num_conns))

try:
    while True:
        events = sel.select(timeout=1)
        if events:
            for key, mask in events:
                service_connection(key, mask)
        # Check for a socket being monitored to continue.
        if not sel.get_map():
            break
except KeyboardInterrupt:
    print("caught keyboard interrupt, exiting")
finally:
    sel.close()