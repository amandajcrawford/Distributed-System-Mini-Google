class HDFS:
    def __init__(self, *args, **kwargs):
        self.phonebook = {}

    def NameNode(self, port):
        # Directory of files that are stored in the distributed system
        # key: Block Number 

        pass
    def DataNode(self, port):
        # self.local_data: {}
        pass

    def writeBlock(self, block_number, document_number, partion_number):
        # check for block_number, document_number, partion_number in self.phonebook (Name Node)
        # if exists - Do Nothing
        # if not exists: Return stream to the datanode where the block should be saved to (Name Node)
        # s1: public/data/hash.txt 
        pass
    
    def completeWrite(self, block_number, document_number, partion_number):
        # NameNode update the self.phonebook with the s1: public/data/hash.txt 