import os
import pathlib
import shutil
import queue
import selectors
import socket
import sys
import numpy as np
import math
import pdb
import time
import threading
import string
from multiprocessing import Process, JoinableQueue, current_process, Lock
from multiprocessing.dummy import Pool as ThreadPool
from base import MasterNode, WorkerNode, MessageBuilder, MessageParser, create_logger
import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize


stop_words = set(stopwords.words('english'))
logger = create_logger()


class IndexWorkerNode(WorkerNode):

    # TASK
    FREE = 9  # Available to start another task
    MAP = 10
    REDUCE = 11

    # Task States
    COMPLETED = 5
    NOT_COMPLETED = 6

    def __init__(self, host, port, worker_num, map_dir, red_dir):
        self.map_dir = map_dir
        self.red_dir = red_dir
        self.task_queue = []
        self.map_task_queue = []
        self.reduce_task_queue = []
        super(IndexWorkerNode, self).__init__(host, port, worker_num)

    def start_worker(self):
        self.curr_task = self.FREE
        self.task_status = self.NOT_COMPLETED

        self.queue_handler = threading.Thread(target=self.queue_handler)
        self.queue_handler.start()

    def handle_request(self, conn, addr, received):
        # Parse Message
        received = received.decode("utf-8")
        parser = MessageParser()
        parsed = parser.parse(received)

        # Get task and add to queue
        task = parsed.action
        if task == 'map':
            self.handle_map_task(parsed)

        if task == 'reduce':
            self.handle_reduce_task(parsed)

    def queue_handler(self):
        self.mapper_free = True
        self.reduce_free = True

        while True:
            try:
                if len(self.map_task_queue) > 0 and self.mapper_free:
                    self.mapper_free = False
                    task_obj = self.map_task_queue[-1]
                    self.map_task(task_obj)
                    self.map_task_queue.remove(task_obj)
                    self.mapper_free = True

                if len(self.reduce_task_queue) > 0 and self.reduce_free:
                    self.mapper_free = False
                    task_obj = self.reduce_task_queue[-1]
                    self.reduce_task(task_obj)
                    self.reduce_task_queue.remove(task_obj)
                    self.mapper_free = True

            except Exception as e:
                print(e)
                break

    def map_task(self, task_obj):
        global stop_words
        task_id = task_obj['task_id']
        dir_path = os.path.abspath(os.path.join(
            os.path.dirname(os.path.abspath(__name__)), '../inputs'))
        input_file_name = task_obj.get("dir").split("/")[-1:][0]
        begin = int(task_obj.get("start"))
        end = int(task_obj.get("end"))
        # indexer_map_dir_path = os.path.join(os.path.dirname(
        #     os.path.abspath(__file__)), os.path.join('indexer', 'map'))
        indexer_map_dir_path = self.map_dir
        base_file_arr = os.path.basename(input_file_name).split(".")
        raw_input_file_name = ""
        if len(base_file_arr) > 0:
            raw_input_file_name = base_file_arr[0]
        else:
            raw_input_file_name = os.path.basename(input_file_name)

        if not os.path.exists(os.path.abspath(os.path.join(indexer_map_dir_path, raw_input_file_name))):
            pathlib.Path(os.path.abspath(os.path.join(
                indexer_map_dir_path, raw_input_file_name))).mkdir(parents=True, exist_ok=True)
        input_file_dir = os.path.abspath(os.path.join(
            indexer_map_dir_path, raw_input_file_name))

        map_file_name = raw_input_file_name + "&Mapper" + str(self.port)
        input_mapper_file = os.path.join(input_file_dir, map_file_name)
        finalArray = []
        try:
            fp = open(task_obj.get("dir"), 'r',
                      encoding="ISO-8859-1")
        except:
            fp = open(task_obj.get("dir"), 'r', encoding="utf-8")

        logger.info('Starting Map Task %s for %s file' %
                    (task_id, map_file_name))
        try:
            partitions = {}
            for i, line in enumerate(fp):
                if i >= begin and i < end:
                    # Remove special characters characters from the line: , . : ; ... and numbers
                    # add 0-9 to re to keep numbers
                    # Make all words to lower case
                    line = re.sub('[^A-Za-z0-9]+', ' ', line).lower()
                    # Tokenize the words into vector of words
                    word_tokens = word_tokenize(line)
                    if len(word_tokens) > 0:
                        for word in word_tokens:
                            if word not in stop_words:
                                first_letter = word[0]
                                if first_letter in partitions.keys():
                                    partitions[first_letter].append(word)
                                else:
                                    partitions[first_letter] = []

                    # Remove non-stop words
                    # finalArray.append(word_tokens)
                    # [finalArray.append(word)
                    #  for word in word_tokens if word not in stop_words]

            for letter, words in partitions.items():
                f = None
                if os.path.exists(os.path.abspath(input_mapper_file + '&' + letter + '.txt')):
                    f = open(os.path.abspath(
                        input_mapper_file + '&' + letter + '.txt'), 'a+')
                    words.sort()
                    [f.write("%s,1\n" % w) for w in words]
                    f.close()
                else:
                    f = open(os.path.abspath(
                        input_mapper_file + '&' + letter + '.txt'), 'w+')
                    words.sort()
                    [f.write("%s,1\n" % w) for w in words]
                    f.close()

            # finalArray.sort()
            # for i in finalArray:
            #     if os.path.exists(os.path.abspath(input_mapper_file + '&' + i[0] + '.txt')):
            #         f = open(os.path.abspath(
            #             input_mapper_file + '&' + i[0] + '.txt'), 'a+')
            #     else:
            #         f = open(os.path.abspath(
            #             input_mapper_file + '&' + i[0] + '.txt'), 'w+')
            #     f.write(i + "," + str(1) + "\n")
            # f.close()
            # fp.close()
        except Exception as e:
            print(e)
        logger.info('Finished Map Task number: %s for %s file' %
                    (task_id, map_file_name))

        # Send complete status to master ----> Need to add in an id for task to
        # map-1

        builder = MessageBuilder(messages=[])
        builder.add_map_complete_message(self.host, self.port, task_id)
        message = builder.build()
        self.master_conn.send(message.outb)
        builder.clear()

    def handle_map_task(self, message):
        directory = message.map_dir
        start = message.map_range_start
        end = message.map_range_end
        t_id = message.taskid
        task_obj = {
            'task': 'map',
            'dir': directory,
            'start': start,
            'end': end,
            'task_id': t_id
        }
        self.map_task_queue.append(task_obj)

    def handle_reduce_task(self, message):
        t_id = message.taskid
        directory = message.red_dir
        start = message.red_start_letter
        end = message.red_end_letter
        task_obj = {
            'task': 'reduce',
            'dir': directory,
            'start': start,
            'end': end,
            'task_id': t_id
        }
        self.reduce_task_queue.append(task_obj)

    def reduce_task(self, task_obj):
        # rootPath = os.path.abspath(os.path.join(
        #     os.path.dirname(os.path.abspath(__name__)), 'indexer/map/'))
        rootPath = self.map_dir
        listDir = os.listdir(rootPath)
        arrayOfWords = []
        start = int(task_obj.get("start"))
        end = int(task_obj.get("end"))
        for directory in listDir:
            files = os.path.abspath(os.path.join(rootPath, directory))
            for mapperFile in os.listdir(files):
                split_y = mapperFile.split("&")[-1:][0]
                letter = split_y.split(".")[0]
                if letter in string.ascii_letters[start:end]:
                    fp = open(os.path.abspath(
                        os.path.join(files, mapperFile)), 'r')
                    count = 0
                    word = ''
                    lastWord = ''
                    arrayOfWords = []
                    for i in fp:
                        word = i.split(",")[0]
                        exists = False
                        for index, i in enumerate(arrayOfWords):
                            if list(i.keys())[0] == word:
                                arrayOfWords[index].update(
                                    {word: arrayOfWords[index].get(word) + 1})
                                exists = True
                                break
                        if not exists:
                            arrayOfWords.append({word: 1})
                    index = 0
                    fp.close()
                    while index < len(arrayOfWords):
                        word = list(arrayOfWords[index].keys())[0]
                        if not os.path.exists(os.path.abspath(os.path.join(os.path.join(self.red_dir, letter + '.txt')))):
                            f = open(os.path.abspath(os.path.join(
                                self.red_dir, letter + '.txt')), "w+")
                            f.close()
                        # fileToWrite = os.path.abspath(os.path.join(os.path.join(os.path.dirname(
                        # os.path.abspath(__name__)), 'indexer'), 'reducers/' +
                        # letter + '.txt'))
                        fileToWrite = os.path.abspath(os.path.join(
                            self.red_dir, letter + '.txt'))
                        # File is empty, write all words starting with letter
                        # w[0]
                        if os.stat(fileToWrite).st_size == 0:
                            f = open(fileToWrite, "a+")
                            lastWord = word
                            while lastWord[0] == word[0] and index < len(arrayOfWords):
                                count = list(arrayOfWords[index].values())[0]
                                lastWord = word
                                f.write(lastWord + " " + mapperFile.split("&")
                                        [0] + ":" + str(count) + '\n')
                                index += 1
                                if index >= len(arrayOfWords):
                                    break
                                word = list(arrayOfWords[index].keys())[0]
                            f.close()
                        # File is not empty. We should load the file and update
                        # older words or add new ones
                        else:
                            lastWord = word
                            f = open(fileToWrite, "r")
                            # Read all document starting with the same letter
                            # as word[0]
                            dictOfFileWords = {"words": {}}
                            for data in f:
                                v = data.strip().split(" ")
                                # pdb.set_trace()
                                filesAndCount = v[1].split(",")
                                tempArray = []
                                for j in filesAndCount:
                                    fc = j.split(":")
                                    tempArray.append(fc[0])
                                    tempArray.append(fc[1])
                                dictOfFileWords["words"].update(
                                    {v[0]: tempArray})
                            while lastWord[0] == word[0] and index < len(arrayOfWords):
                                # word exists in existed file?
                                if dictOfFileWords["words"].get(word):
                                    if mapperFile.split("&")[0] in dictOfFileWords["words"].get(word):
                                        indexInArray = dictOfFileWords["words"].get(
                                            word).index(mapperFile.split("&")[0])
                                        dictOfFileWords["words"][word][indexInArray + 1] = int(dictOfFileWords["words"][
                                            word][indexInArray + 1]) + int(list(arrayOfWords[index].values())[0])
                                    else:
                                        # Yes, add to the array list the name
                                        # of the file that are being read and
                                        # the count
                                        y = dictOfFileWords["words"].get(word)
                                        # Append the name of the file
                                        y.append(mapperFile.split("&")[0])
                                        # Append the count of the word
                                        y.append(
                                            list(arrayOfWords[index].values())[0])
                                        dictOfFileWords[
                                            "words"].update({word: y})
                                else:
                                    dictOfFileWords["words"].update(
                                        {word: [mapperFile.split("&")[0], list(arrayOfWords[index].values())[0]]})
                                index += 1
                                if index >= len(arrayOfWords):
                                    break
                                lastWord = word
                                word = list(arrayOfWords[index].keys())[0]
                            f.close()
                            f = open(fileToWrite, "w")
                            for item, array in dictOfFileWords["words"].items():
                                filesAndCount = ''
                                i = 0
                                while i < len(array):
                                    filesAndCount += array[i] + \
                                        ":" + str(array[i + 1]) + ","
                                    i += 2
                                filesAndCount = filesAndCount[:-1] + '\n'
                                # word + ' ' + arrays
                                line = item + ' ' + filesAndCount
                                f.write(line)
        builder = MessageBuilder(messages=[])
        builder.add_reduce_complete_message(
            self.host, self.port, task_obj.get("task_id"))
        message = builder.build()
        self.master_conn.send(message.outb)
        builder.clear()


class IndexMasterNode(MasterNode):
    # Job Stages
    PARTIOINING = 5
    MAP = 2
    REDUCE = 3
    COMPLETE = 18

    # Job Completion States
    NOT_STARTED = 4
    IN_PROGRESS = 5
    COMPLETED = 6

    # Overall Indexing States
    RUNNING = 7
    FINISHED = 8

    def __init__(self, host, port, worker_num, index, map_dir, red_dir):
        super(IndexMasterNode, self).__init__(host, port, worker_num)

        self.index_dir = index
        self.map_dir = map_dir
        self.red_dir = red_dir
        # Handles iterating through job stages
        self.curr_job = self.PARTIOINING  # Partition or Map or Reduce
        self.job_status = self.NOT_STARTED
        self.index_status = self.RUNNING

        # Index File
        self.index_files = None

        # Map and Reduce Task Information
        self.map_taskmap = []  # array of map_task_id
        self.reduce_taskmap = []

    def start_master(self):
        # Starting Master and Background processes
        master_bg = threading.Thread(target=self.master_bg)
        master_bg.start()

    def master_bg(self):
        while True:
            # print(self.reduce_taskmap)
            if self.index_files is None and self.curr_job == self.PARTIOINING:
                # Can begin index partitioning work
                self.index_files = self.populateArrayOfFilesAndSize(
                    self.index_dir)
                self.curr_job = self.MAP
                # Check to see if we have all the workers connected and ready

            if self.worker_status == self.ALL_CONNECTED:

                if (self.curr_job == self.MAP) and (self.job_status == self.NOT_STARTED) and self.index_files is not None:
                    logger.info(
                        'Starting MAP++++++++ Index Files For Worker Nodes Map Tasks')
                    self.job_status = self.IN_PROGRESS
                    self.distributeJobToMappers(self.index_files)

                if self.curr_job == self.REDUCE and self.job_status == self.NOT_STARTED:
                    logger.info(
                        'Starting REDUCE++++++++ Index Files For Worker Nodes Reduce Tasks')
                    self.job_status = self.IN_PROGRESS
                    # self.distributeJobToReducers(os.path.join(
                    #     os.path.dirname(__file__), 'indexer/map'))
                    self.distributeJobToReducers(self.map_dir)

                if self.curr_job == self.REDUCE and self.job_status == self.NOT_STARTED:
                    logger.info('Updating pointing index directory')

        return

    def handle_request(self, conn, addr, received):
        # Call this first to make sure worker nodes are being added to list
        super().handle_request(conn, addr, received)

        # Parse the message received from sender
        received = received.decode("utf-8")
        parser = MessageParser()
        parsed = parser.parse(received)
        if parsed.action == 'map' and parsed.status == 'complete':
            task_id = parsed.taskid
            self.map_taskmap.remove(task_id)

            # Check to see if all map jobs are clear and can start reduce phase
            if len(self.map_taskmap) == 0:
                self.curr_job = self.REDUCE
                self.job_status = self.NOT_STARTED
                logger.info('FINISHED MAP+++++++')

        if parsed.action == 'reduce' and parsed.status == 'complete':
            task_id = parsed.taskid
            self.reduce_taskmap.remove(task_id)

            # Check to see if all reduce jobs are clear and can start reduce
            # phase
            if len(self.reduce_taskmap) == 0:
                self.curr_job = self.COMPLETE
                self.job_status = self.NOT_STARTED
                logger.info('FINISHED REDUCE+++++++')

    def populateArrayOfFilesAndSize(self, path):
        files = os.listdir(os.path.abspath(path))
        arrayOfFilesAndSize = np.empty([1, 0])
        for iFile in files:
            pathToFile = os.path.join(
                path,
                iFile,
            )
            f = open(os.path.abspath(pathToFile), encoding="ISO-8859-1")
            lines = 0
            buf_size = 1024 * 1024
            read_f = f.read
            buf = read_f(buf_size)
            while buf:
                lines += buf.count("\n")
                buf = read_f(buf_size)
            f.close()
            arrayOfFilesAndSize = np.append(
                arrayOfFilesAndSize, [{pathToFile: lines}])
        return arrayOfFilesAndSize

    def distributeJobToMappers(self, arrayOfDictionariesOfFilesPaths):
        """
            Function receives an array of dictionaries where each dictionary has the form:
            'path/to/file': int number_of_lines}
            The function then distributes each block to each Mapper
        """
        count = 0
        worker_keys = list(self.worker_conns.keys())
        builder = MessageBuilder(messages=[])
        for i in arrayOfDictionariesOfFilesPaths:
            count += 1
            k = i
            fi = list(k.keys())[0]
            k = list(k.values())[0]
            lines = k
            t = math.ceil(k / self.num_workers)
            worker, y = 0, 0
            while (lines - t) > 0:
                k = y
                y += t
                task_id = 'map-' + str(count) + str(y)
                builder.add_task_map_message(
                    self.host, self.port, task_id, fi, k, y)
                message = builder.build()
                worker_conn = self.worker_conns[worker_keys[worker]]
                worker_conn.send(message.outb)
                self.map_taskmap.append(task_id)
                lines -= t
                worker += 1

            task_id = 'map-' + str(count) + str(y + 1)
            builder.add_task_map_message(
                self.host, self.port, task_id, fi, y, (list(i.values())[0]))
            message = builder.build()
            worker_conn = self.worker_conns[worker_keys[worker]]
            worker_conn.send(message.outb)
            self.map_taskmap.append(task_id)

    def distributeJobToReducers(self, path):
        files = os.listdir(path)
        builder = MessageBuilder(messages=[])
        letterPerWorker = math.ceil((26 / self.num_workers))
        worker_keys = list(self.worker_conns.keys())
        # dire = os.path.join(os.path.dirname(
        #     os.path.abspath(__name__)), 'indexer/map/')
        dire = self.map_dir
        count = 0
        y = 0
        worker, letters = 0, 26
        pathToSend = 'empty'
        while (letters - letterPerWorker) > 0:
            k = y
            y += letterPerWorker
            task_id = 'reduce-' + str(count) + str(y)
            builder.add_task_reduce_message(
                self.host, self.port, task_id, pathToSend, k, y)
            message = builder.build()
            builder.clear()
            worker_conn = self.worker_conns[worker_keys[worker]]
            worker_conn.send(message.outb)
            letters -= letterPerWorker
            worker += 1
            count += 1
            self.reduce_taskmap.append(task_id)
        task_id = 'reduce-' + str(count) + str(y + 1)
        builder.add_task_reduce_message(
            self.host, self.port, task_id, pathToSend, y, 26)
        message = builder.build()
        builder.clear()
        worker_conn = self.worker_conns[worker_keys[worker]]
        worker_conn.send(message.outb)
        self.reduce_taskmap.append(task_id)


class IndexCluster:

    def __init__(self, master_addr, worker_num, index_dir):
        if not master_addr:
            self.master_addr = ("localhost", 8956)
        else:
            self.master_addr = master_addr

        if not worker_num:
            return RuntimeError("Worker nodes not defined")

        self.host = self.master_addr[0]
        self.master_port = self.master_addr[1]
        self.nodes = {"master": master_addr, "workers": []}
        self.worker_num = worker_num
        self.index_dir = index_dir

    def setup_index_dir(self):
        # Create temp directory for map jobs
        index_root = os.path.join(os.path.dirname(__file__), 'indexer_1')
        if os.path.exists(index_root):
            # check to see if index update file exists
            up_file = os.path.join(index_root, "updated.txt")
            if os.path.exists(up_file):
                with open(up_file, "r") as up_filep:
                    # read line
                    old_indx = up_filep.readline().strip()
                    basename = os.path.basename(old_indx).split("_")
                    last_int = str(int(basename[-1]) + 1)
                    new_index = "indexer_" + last_int
            else:
                new_index = "indexer_2"

            up_filep = open(up_file, "w+")
            index_root = os.path.abspath(os.path.join(
                os.path.dirname(__file__), new_index))
            up_filep.write(index_root)
        else:
            pathlib.Path(index_root).mkdir(exist_ok=True)
        # self.index_map = os.path.join(os.path.dirname(__file__),
        # 'indexer/map')
        self.index_map = os.path.join(index_root, 'map')
        # shutil.rmtree(self.index_map, ignore_errors=True)
        if not os.path.exists(self.index_map):
            pathlib.Path(self.index_map).mkdir(parents=True, exist_ok=True)
        else:
            pathlib.Path(self.index_map).mkdir(parents=True, exist_ok=True)

        # Create directory for reduce job and inverted index
        # self.index_reduce = os.path.join(
        #     os.path.dirname(__file__), 'indexer/reducers')
        self.index_reduce = os.path.join(index_root, 'reduce')
        # shutil.rmtree(self.index_reduce, ignore_errors=True)
        # if not os.path.exists(self.index_reduce):
        pathlib.Path(self.index_reduce).mkdir(parents=True, exist_ok=True)

    def start(self):
        nodes = []

        # Setup index directory for map reduce task
        self.setup_index_dir()

        # Create Master Node and start process
        master = IndexMasterNode(self.master_addr[0], self.master_addr[
            1], self.worker_num, self.index_dir, self.index_map, self.index_reduce)
        nodes.append(master)
        master.start()

        # Load addresses for worker nodes
        addr_list = []
        with open(os.path.join(os.path.dirname(__file__), 'hosts.txt'), 'r', encoding="ISO-8859-1") as f:
            for line in f:
                l = line.strip().split(' ')
                if len(l) == 2:
                    addr = (l[0], int(l[1]))
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                        res = sock.connect_ex(addr)
                        if res != 0:  # Port is not in use by another process
                            addr_list.append(addr)  # add as available

        # Start worker nodes and append to list
        worker_addr = []
        for i in range(self.worker_num):
            addr = addr_list[i]
            worker_addr.append(addr)
            host = addr[0]
            port = int(addr[1])
            node = IndexWorkerNode(
                host, port, self.master_addr, self.index_map, self.index_reduce)
            nodes.append(node)
            node.start()

        # Set worker addresses
        self.nodes['workers'] = worker_addr

        for node in nodes:
            node.join()


class IndexClient:

    def __init__(self, index_dir, num_nodes, host='localhost', port=9803):
        self.num_nodes = num_nodes
        self.index_dir = index_dir
        self.master_host = host
        self.master_port = int(port)

    def start(self):
        ip = (self.master_host, self.master_port)
        inx = IndexCluster(ip, self.num_nodes, self.index_dir)
        inx.start()

if __name__ == "__main__":
    input_dir = os.path.join(os.path.dirname(
        os.path.abspath(__name__)), 'inputs')
    inx = IndexClient(input_dir, 2, host='localhost', port=9859)
    inx.start()
