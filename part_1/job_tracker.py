class JobTracker:
    # Get list of data nodes in hdfs and the size of their storage
    # Create a task for each worker nodes - Task Tracker
    # Map Task: # of mappers
    # Combine/Shuffle Task: # of shufflers
    # Reduce Task: # of reducers
    pass

class TaskTracker:

    # Receive a task
    # Execute the task/ Update the JobTracker of progress including failures of a mapper (heartbeat)
    # Tells the JobTracker when complete
    # Receives the next task 

    # Map Task Execution:
    # input to each mapper: # of mappers / data node blocks 
    # call mappers in parallel with the appropiate inputs 

    # Combine/Shuffle Task:
    # # of shufflers/ mappers
    # call shufflers with input 

    # Reduce/Task
    # Find the input to each reducer
    # Run the reducer 


    pass