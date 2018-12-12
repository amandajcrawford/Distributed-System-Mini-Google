import sys,os,re,string
import numpy as np
import pdb
#Each reducer is responsible by a group of keys

def writeToCorrespondentFile():
    pass

begin,end = 'a','c'
#Reducer should go insider folder of respective file
path = '/home/raphael/Documents/distributed-mine/mapperDirectory/inputtest1'
files = os.listdir(os.path.abspath(path))
arrayOfWords = []
for k in files:
    fp = open(os.path.abspath(os.path.join(path,k)),'r')
    count = 0
    word = ''
    lastWord = ''
    arrayOfWords = []
    for i in fp:
        word = i.split(",")[0]
        if i[0] in string.ascii_letters:
            #Word does not exist in the arrayOfWords, then add the word with count = 1
            if not any(d.get(word) for d in arrayOfWords):
                arrayOfWords.append({word:1})
            else:
                #Because we always append at the end of the list. Just increment the last position
                arrayOfWords[-1][word]+=1
        else:
            break
    index = 0
    while index < len(arrayOfWords):
        word = list(arrayOfWords[index].keys())[0]
        if not os.path.exists(os.path.abspath(os.path.join('/home/raphael/Documents/distributed-mine/','reducers/'+word[0]+'.txt'))):
            f = open(os.path.abspath(os.path.join('/home/raphael/Documents/distributed-mine/','reducers/'+word[0]+'.txt')),"w+")
            f.close()
        fileToWrite = os.path.abspath(os.path.join('/home/raphael/Documents/distributed-mine/','reducers/'+word[0]+'.txt'))
        #File is empty, write all words starting with letter w[0]
        if os.stat(fileToWrite).st_size == 0:
            f = open(fileToWrite,"a+")
            lastWord = word
            while lastWord[0] == word[0] and index < len(arrayOfWords):
                count = list(arrayOfWords[index].values())[0]
                lastWord = word
                f.write(lastWord + " " + k.split("|")[0] + ":" + str(count) + '\n')
                index+=1
                if index >= len(arrayOfWords):
                    break
                word = list(arrayOfWords[index].keys())[0]
            f.close()
        #File is not empty. We should load the file and update older words or add new ones
        else:
            lastWord = word 
            f = open(fileToWrite,"r")
            #Read all document starting with the same letter as word[0]            
            dictOfFileWords = {"words":{}}
            for data in f:
                v = data.strip().split(" ")
                # pdb.set_trace()
                filesAndCount = v[1].split(",")
                tempArray = []
                for j in filesAndCount:
                    fc = j.split(":")
                    tempArray.append(fc[0])
                    tempArray.append(fc[1])
                dictOfFileWords["words"].update({v[0]:tempArray})
            while lastWord[0] == word[0] and index < len(arrayOfWords):
                #word exists in existed file?
                if dictOfFileWords["words"].get(word):
                    if k.split("|")[0] in dictOfFileWords["words"].get(word):
                        indexInArray = dictOfFileWords["words"].get(word).index(k.split("|")[0])
                        dictOfFileWords["words"][word][indexInArray+1] = int(dictOfFileWords["words"][word][indexInArray+1]) + int(list(arrayOfWords[index].values())[0])
                    else:
                        #Yes, add to the array list the name of the file that are being read and the count
                        y = dictOfFileWords["words"].get(word)
                        #Append the name of the file
                        y.append(k.split("|")[0])
                        #Append the count of the word
                        y.append(list(arrayOfWords[index].values())[0])
                        dictOfFileWords["words"].update({word:y})
                else:
                    dictOfFileWords["words"].update({word:[k.split("|")[0],list(arrayOfWords[index].values())[0]]})
                index+=1
                if index >= len(arrayOfWords):
                    break
                lastWord = word
                word = list(arrayOfWords[index].keys())[0]
            f.close()
            f = open(fileToWrite,"w")
            for item,array in dictOfFileWords["words"].items():
                filesAndCount = ''
                i = 0
                while i < len(array):
                    filesAndCount+=array[i] + ":" + str(array[i+1]) + ","
                    i+=2
                filesAndCount = filesAndCount[:-1] + '\n'
                #word + ' ' + arrays
                line = item + ' ' + filesAndCount
                f.write(line)
