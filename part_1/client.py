import logging 
import socket 
import multiprocessing


s = socket.socket()
port = 45000
s.connect(('127.0.0.1', port))
s.send(b"RPC:/home/raphael/Documents/distributedSystems/Distributed-System-Mini-Google/inputs")
x = s.recv(4096)
s.close()