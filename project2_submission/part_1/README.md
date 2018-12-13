# 1. Indexer System

## 1.1. How to Run

```python indexer.py -fs [input_directory] -nodes [nodes] -host [system_host] -port [master_port]```

# 2. Search System Overview

```mermaid

sequenceDiagram
    participant Search Query Master
    participant Search Client
    participant Search Worker Helper
    Search Client ->> Search Query Master: Hello
    Search Query Master ->> Search Client: I got it

```

## 2.1. How to Run

```python search.py -index [input_directory] -nodes [nodes] -host [system_host] -port [master_port]```
