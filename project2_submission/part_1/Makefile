# Project 2: Distributed Mini Google System 
# CS2510 - Distributed Operating Systems
# Amanda Crawford and Raphael Fernandes

# Project Dependencies
# --- Python 3.6
# ---- psutils python library
# -----nltk python library 
# -----nltk corpus needed : stopwords, punkt 
#
install:
	pip install psutils
	pip install nltk


# 	### Index System Commands #####
# 	Run the indexer system 
# 

index: indexer.py
	export LD_LIBRARY_PATH=/usr/local/lib
	python3 indexer.py > indexer.py

index: indexer.py
	export LD_LIBRARY_PATH=/usr/local/lib
	python3 indexer.py > indexer.py

# 	### Search System Commands #####
# 	To run the search system, the master node must be running and listening or the worker nodes will not be able to start.To run the sea
# 
search_master: search_master.py
	export LD_LIBRARY_PATH=/usr/local/lib
	python3 search_master.py

search_worker:
	export LD_LIBRARY_PATH=/usr/local/lib
	python3 search_worker.py

search_worker_hydrogen:
	export LD_LIBRARY_PATH=/usr/local/lib
	python3 search_worker.py -m_host hydrogen.cs.pitt.edu

search_worker_hydrogen:
	export LD_LIBRARY_PATH=/usr/local/lib
	python3 search_worker.py -m_host hydrogen.cs.pitt.edu

search_tester:
	export LD_LIBRARY_PATH=/usr/local/lib
	python search_tester.py keywords_search.txt localhost 9878 2