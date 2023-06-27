import threading
import time
import statistics

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
                # print("Node", self.node_id, "results:", results)
                time.sleep(word_count)
                # print("Node", self.node_id, "finished processing task:", task)
            else:
                time.sleep(1)

        # print("Node", self.node_id, "stopped")


class NodeManager:
    def __init__(self, max_tasks,preset_time, num_nodes):
        self.nodes = [Node(i, max_tasks) for i in range(num_nodes)]
        self.max_tasks = max_tasks
        self.preset_time = preset_time
        for node in self.nodes:
            node.start()

    def get_nodes(self):
        return self.nodes
    #
    # def create_node(self):
    #     node = Node(len(self.nodes), self.max_tasks)
    #     node.start()
    #     self.nodes.append(node)
    #     return node


class TaskScheduler:
    def __init__(self, node_manager):
        self.node_manager = node_manager


    def assign_task(self, tasks):
        for task in tasks:
            word_count = len(task.split())
            nodes = self.node_manager.get_nodes()
            for node in nodes:
                cur_time = sum(len(task.split()) for task in node.tasks)
                with node.lock:
                    if len(node.tasks) < node.max_tasks and cur_time + word_count <= self.node_manager.preset_time:
                        node.tasks.append(task)
                        print("Assigned task:", task, "to node", node.node_id)
                        # assigned = True
                        break
            else:
                print("this task", task, "can not assign(oversize or not enough nodes)")


data = ["task1 happy join happy", "task2 like hi hi hi hi hi", "task3 like dede", "task4 hello hi", "task5 hi", "task6","task7 Once upon a time","task8 in a faraway land there","task9 was a tiny","task11 kingdom"," task12 peaceful prosperous and rich in romance oh", "task13 and tradition nestled in","task14 the heart of","task15 this kingdom was a small village with","task16 an enchanted castle towering high above the clouds the","task17 townsfolk of this village lived a simple life but","task18 they","task19 lived","task20 in fear for","task21 in this castle lived a beast","task22 unlike any other a horrible creature with a","task23 mean streak two","task24 miles wide the villagers lived in terror of","task25 the","task26 beast and would not dare to approach the castle"]
max_tasks = 2
preset_time = 12
num_nodes = 26

Node_manager = NodeManager(max_tasks,preset_time,num_nodes)
scheduler = TaskScheduler(Node_manager)
scheduler.assign_task(data)
assigned_nodes = [node for node in Node_manager.get_nodes() if node.tasks]
print(f"{len(assigned_nodes)} nodes have been assigned tasks.")
task_counts = [len(node.tasks) for node in Node_manager.get_nodes() if len(node.tasks) > 0]
stdev_task_count = statistics.stdev(task_counts)
print("stdev task count per node (only nodes with tasks):", stdev_task_count)

while any(node.tasks for node in Node_manager.get_nodes()):
    time.sleep(1)

for node in Node_manager.get_nodes():
    node.should_stop = True
for node in Node_manager.get_nodes():
    node.join()

# for task in data:
#     print(len(task.split()))
