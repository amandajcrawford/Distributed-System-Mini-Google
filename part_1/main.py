
import logging 

def main():
    # Get Cluster Node Count 
        # Setup Servers For Index and Search

    # Get Command Type: Index or Search

    ## Index Command {argument: input files}
    # Get input files from User
    # Load HDFS 
    # Create a new Job using the job tracker- 1. Load HDFS(name node1, chapter, books) 2. Map 3. shuffle 4. reduce (Load HDFS Output/ Name Node2) ( inverted index)
    # Return when finished

    # Search Command (argument: keywords)
    # Add query to search queue
    # Find document keywords
    # Aggregate and rank results
    # Print results
    return

    
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    main()
    