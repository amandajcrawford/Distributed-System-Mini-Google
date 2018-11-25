#  HDFS
# Create/ Write
# Read

#Loading HDFS
1. Take list of input files
2. Splitting each document amongst the number of data nodes

#Reading from HDFS to Mappers
1. Block 


# Storage Partitioning (Create/ Write Operation)
1. By Size
For each Document:
    Split by Number of Lines / Number of bytes
    Assign partition to a data node server

Example:
System: 
3 Data Servers- S1, S2, S3 
All Servers can store up to 5000

Input:
5 input files: D1, D2, D3, D4, D5
Each file with a size(line\bytes\mb\gb)
D1 - 500
D2 - 1000
D3 - 4000
D4 - 10
D5 - 100

Partion by Size among Data Server , n = 3
Solution( partition by file_size\128)

Partition
D1 (p=1) - D1P1
D2 (p=2) - D2P1(1-128) D2P2(129-256)
D3 (p=8) - D3P1....D3P8
D4 (p=1) - D4P1
D5 (p=1) - D5P1

Store Partion to Server
S1 - D1P1, D4P1, D3P5-D3P7
S2 - D3P1 - D3P4
S3 - D3P8, D2P1, D2P2, D5P1

Name Node Directory (Without Replication)
D1 - P1:S1
D2 - P1:S3, P2:S3
D3 - P1:S2, P2:S2, P3:S2, P4:S2, P5:S1, P6:S1, P7:S1, P8:S3
D4 - P1:S1
D5 - P1:S3

Name Node Directory (With Replication)
D1 - P1:(S1, S7, S9)
D2 - P1:S3, P2:S3
D3 - P1:S2, P2:S2, P3:S2, P4:S2, P5:S1, P6:S1, P7:S1, P8:S3
D4 - P1:S1
D5 - P1:S3


