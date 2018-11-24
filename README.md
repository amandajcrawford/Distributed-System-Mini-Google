Distributed-System-Mini-Google
===========
Project number two of Distributed System course at Pitt for graduate students.


 __Amanda Crawford__: [acrawfor](https://github.com/acrawfor)

 __Raphael__


1. Project Overview
2. Mini Google Overview
3. Hadoop
    1. Hadoop Phases
    2. Hadoop Cluster
4. Hadoop Communication Paradigmns
    1. Job Tracker - Task Tracker
5. HDFS Design Considerations
    1. HDFS Operations
    2. Components

# Project Overview


# Mini Google Overview
__1.__ Hadoop Implementation 
* HDFS
* MapReduce Engine
* HBase / Lucene ( will not be used in project but influences designs)

__2.__ Alternate Implementation 
* Distributed File System - Data Storage and Batch Processing
* MapReduce - Computing 
* Inverted Index - Searching and Query Handling

# Hadoop  
## Hadoop Phases 
1. Map
2. Sort/ Shuffle / Aggregate
3. Reduce 
   

## Hadoop Cluster
![yarn](https://2xbbhjxc6wk3v21p62t8n4d4-wpengine.netdna-ssl.com/wp-content/uploads/2012/08/yarnflow1.png)
* Master Nodes
    * Yarn Resource Manager
    * HDFS Name Node
    * Job Trackers 
    * Work Queue
* Worker Nodes
    * Yarn NodeManager 
    * HDFS DataNode
    * Task Tracker and tasks

# Hadoop Communication Paradigmns

## Job Tracker - Task Tracker 
* ___Task Trackers__ 
    * create and remove tasks received from the job trackers 
    * communicates task status to job tracker by sending heartbeats
* ___Job Tracker__
    * Manages task trackers 
    * Schedule and tracks jobs progress
    * Receives jobs from clients



# HDFS Design Considerations
![hdfsarchitecture](/assets/hdfsarchitecture.gif)
Source: https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html



## HDFS Operations
* Read - Load data 

## Components
* Name Node 
* Data Node
* Task Tracker 


Sources:
http://www-scf.usc.edu/~shin630/Youngmin/files/HadoopInvertedIndexV5.pdf

[Hadoop YARN](https://hortonworks.com/blog/apache-hadoop-yarn-resourcemanager/)

[Hadoop HDFS](https://hortonworks.com/blog/thinking-about-the-hdfs-vs-other-storage-technologies/)

[Hadoop HDFS] (https://www.tutorialspoint.com/hadoop/hadoop_hdfs_overview.htm)

[Searching HDFS](http://www.drdobbs.com/parallel/indexing-and-searching-on-a-hadoop-distr/226300241?pgno=3)

[HBase](https://www.tutorialspoint.com/hbase/hbase_overview.htm)

[MapReduce] (https://www.guru99.com/introduction-to-mapreduce.html)

[MapReduce] (https://hci.stanford.edu/courses/cs448g/a2/files/map_reduce_tutorial.pdf)

[MapReduce] (https://www.tutorialspoint.com/hadoop/hadoop_mapreduce.htm)
