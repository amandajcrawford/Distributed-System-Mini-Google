# Project 2: Distributed Mini Google System 
# CS2510 - Distributed Operating Systems
# Amanda Crawford and Raphael Fernandes

# Project Dependencies
# --- Python 3.6
# ---- psutils python library
# -----nltk python library 
# -----nltk corpus needed : stopwords, punkt 
#

SM_PORT = 13086
SM_HOST = "localhost"
HOST = "localhost"
INODES = 4

install:
	export LD_LIBRARY_PATH=/usr/local/lib
	pip3 install psutil --user
	pip3 install nltk --user


# 	### Index System Commands #####
# 	Run the indexer system 
# 

index: indexer.py
	python3 indexer.py -nodes $(INODES)


# 	### Search System Commands #####
# 	To run the search system, the master node must be running and listening or the worker nodes will not be able to start.To run the sea
# 
search:
	python3 keyword_search.py $(SM_HOST) $(SM_PORT) --key_words $(KW)

search_master: search_master.py
	python3 search_master.py -port $(SM_PORT) 

search_worker:
	python3 search_worker.py -m_host $(SM_HOST) -m_port $(SM_PORT) 

search_tester:
	python search_tester.py keywords_search.txt localhost $(SM_PORT) $(clients)

clean:
	rm -r __*
	rm -r *.log*

clean_index: 
	rm -r /indexer

clean_all:
	rm -r /indexer
	rm *.log