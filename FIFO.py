
import threading
import time

def MapFunction(data):
    cur_results = []
    words = data.split()
    for word in words:
        cur_results.append((word, 1))
    return cur_results

def ReduceFunction(key, values):
    result = sum(values)
    return (key, result)

def MapReduce(data):
    cur_results = []
    for sentence in data:
        cur_results.extend(MapFunction(sentence))

    FinalResults = []
    key_values = {}
    for key, value in cur_results:
        if key not in key_values:
            key_values[key] = []
        key_values[key].append(value)

    for key, values in key_values.items():
        FinalResults.append(ReduceFunction(key, values))

    return FinalResults


class Node(threading.Thread):
    def __init__(self, node_id, max_tasks):
        threading.Thread.__init__(self)
        self.node_id = node_id
        self.tasks = []
        self.should_stop = False
        self.max_tasks = max_tasks
        self.lock = threading.Lock()

    def run(self):
        print("Node", self.node_id, "started")
        while not self.should_stop:
            task = None
            with self.lock:
                if len(self.tasks) > 0:
                    task = self.tasks.pop(0)  # get a task

            if task:  # make sure there is a task to process
                # process the task
                results = MapReduce([task])  # process the task
                print("Node", self.node_id, "results:", results)
                time.sleep(10)  # simulate time-consuming task
                print("Node", self.node_id, "finished processing task:", task)
            else:
                time.sleep(1)  # if there is no task, let the thread sleep for a while to avoid busy waiting

        print("Node", self.node_id, "stopped")


class TaskScheduler:
    def __init__(self, nodes):
        self.nodes = nodes

    def assign_task(self, tasks):
        # Assign each task to a node
        for task in tasks:
            assigned = False  # whether the task has been assigned
            while not assigned:  # keep trying until the task is assigned
                for node in self.nodes:
                    with node.lock:
                        if len(node.tasks) < node.max_tasks:  # find a node with capacity
                            node.tasks.append(task)  # assign the task
                            print("Assigned task:", task, "to node", node.node_id)
                            assigned = True
                            break
                if not assigned:  # if no node with capacity is found
                    time.sleep(1)  # wait for a while before trying again


data = ["task1 happy join happy", "task2 like hi hi hi hi", "task3 like dede", "task4 hello hi", "task5 hi"]
num_nodes = 2
max_tasks = 2

nodes = [Node(i, max_tasks) for i in range(num_nodes)]

for node in nodes:
    node.start()
scheduler = TaskScheduler(nodes)
scheduler.assign_task(data)

# wait for all tasks to be finished
while any(node.tasks for node in nodes):
    time.sleep(1)

# stop nodes
for node in nodes:
    node.should_stop = True
for node in nodes:
    node.join()
