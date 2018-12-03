from multiprocessing import Process
from base import logger

class MapReduceProcess(Process):
    def __init__(self, task_queue):
        self.task_queue = task_queue
        super(MapReduceProcess, self).__init__()

    def run(self):
        while True:
            if not self.task_queue.empty():
                task = self.task_queue.get_nowait()
                logger.info("Starting Task:"+repr(task)+"")
                task_type = task['task']
                if task_type == 'map':
                    self.run_map(task)

                if task_type == 'reduce':
                    self.run_reduce(task)

    def run_map(self, task):
        # Get number of mappers
        mappers = task.mappers['mappers']

        
        # Assign block for each mapper

        # Run map job

        # Save map to map output directory



    def run_reduce(self, task):
        #Get number of reducers
        reducers = task["reducers"]

        #Assign map files to reducers

        #Save file to reducer output directory