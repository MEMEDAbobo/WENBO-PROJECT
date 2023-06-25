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
    total_words = sum([len(sentence.split()) for sentence in data])
    return FinalResults,total_words


class Node(threading.Thread):
    def __init__(self, node_id, max_tasks):
        threading.Thread.__init__(self)
        self.node_id = node_id
        self.tasks = []
        self.should_stop = False
        self.max_tasks = max_tasks
        self.lock = threading.Lock()

    def run(self):
        while not self.should_stop:
            task = None
            with self.lock:
                if len(self.tasks) > 0:
                    task = self.tasks.pop(0)

            if task:
                results,word_count = MapReduce([task])
                print("Node", self.node_id, "results:", results)
                time.sleep(word_count)
                # print("Node", self.node_id, "finished processing task:", task)
            else:
                time.sleep(1)

        print("Node", self.node_id, "stopped")


class NodeManager:
    def __init__(self, max_tasks):
        self.nodes = []
        self.max_tasks = max_tasks

    def get_nodes(self):
        return self.nodes

    def create_node(self):
        node = Node(len(self.nodes), self.max_tasks)
        node.start()
        self.nodes.append(node)
        return node


class TaskScheduler:
    def __init__(self, node_manager,preset_time):
        self.node_manager = node_manager
        self.preset_time = preset_time

    def assign_task(self, tasks):
        for task in tasks:
            assigned = False
            word_count = len(task.split())
            while not assigned:
                nodes = self.node_manager.get_nodes()
                for node in nodes:
                    cur_time = 0
                    for i in node.tasks:
                        cur_time = len(i.split()) + cur_time
                    with node.lock:
                        if len(node.tasks) < node.max_tasks and cur_time + word_count <= self.preset_time:
                            node.tasks.append(task)
                            print("Assigned task:", task, "to node", node.node_id)
                            assigned = True
                            break
                if not assigned:
                    print("No node or All nodes are busy, creating a new one...")
                    new_node = self.node_manager.create_node()
                    with new_node.lock:
                        new_node.tasks.append(task)
                        print("Assigned task:", task, "to newly created node", new_node.node_id)
                        assigned = True


data = ["task1 happy join happy", "task2 like hi hi hi hi", "task3 like dede", "task4 hello hi", "task5 hi"]
max_tasks = 2

node_manager = NodeManager(max_tasks)
scheduler = TaskScheduler(node_manager,9)
scheduler.assign_task(data)

while any(node.tasks for node in node_manager.get_nodes()):
    time.sleep(1)

for node in node_manager.get_nodes():
    node.should_stop = True
for node in node_manager.get_nodes():
    node.join()
