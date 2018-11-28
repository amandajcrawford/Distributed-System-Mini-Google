from multiprocessing import Manager, Process, current_process


class Cluster:
    pass

class Task:
    pass

def worker(aList):
    aList[int(current_process().name.split("-")[1]) -1] = (int(current_process().name.split("-")[1]) -1) * (int(current_process().name.split("-")[1]) -1)
if __name__ == "__main__":
    n = 10

    manager = Manager()

    l = manager.list()

    l.extend(range(n))

    p = [Process(target=worker, args=(l, )) for i in range(n-1)]

    for each in p:
        each.start()

    for each in p:
        each.join()

    print("final: ",l)