import threading
import queue
import time

class Node(threading.Thread):
    def __init__(self, node_id, task_queue):
        threading.Thread.__init__(self)
        self.node_id = node_id
        self.task_queue = task_queue

    def run(self):
        print("Node", self.node_id, "started")
        while True:
            task = self.task_queue.get()  # get a task
            if task is None:  # None is our signal to stop the thread
                break
            print("Node", self.node_id, "processing task:", task)
            time.sleep(10)  # simulate time-consuming task
            print("Node", self.node_id, "finished processing task:", task)
            self.task_queue.task_done()  # signal that the task is done

class TaskScheduler:
    def __init__(self, nodes, task_queue):
        self.nodes = nodes
        self.task_queue = task_queue

    def assign_task(self, tasks):
        # Assign each task to a node
        for task in tasks:
            self.task_queue.put(task)  # put the task in the queue

data = ["task1 happy join happy", "task2 like hi hi hi hi", "task3 like dede", "task4 hello hi", "task5 hi"]
num_nodes = 2

task_queue = queue.Queue()  # create a Queue
nodes = [Node(i, task_queue) for i in range(num_nodes)]

for node in nodes:
    node.start()
scheduler = TaskScheduler(nodes, task_queue)
scheduler.assign_task(data)

task_queue.join()  # wait for all tasks to be done

for i in range(num_nodes):
    task_queue.put(None)  # signal the nodes to stop
for node in nodes:
    node.join()
