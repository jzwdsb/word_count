import unittest
from worker import *
from driver import *

class TestAssignTasks(unittest.TestCase):
    def test_assign_tasks(self):
        input_files = ["file1.txt", "file2.txt", "file3.txt"]
        num_map_tasks = 4
        num_reduce_tasks = 2

        tasks = assign_tasks(input_files, num_map_tasks, num_reduce_tasks)

        self.assertEqual(tasks.qsize(), num_map_tasks + num_reduce_tasks)

        # Check map tasks
        for i in range(num_map_tasks):
            task = tasks.get()
            print("task: ", json.dumps(task).encode())
            self.assertEqual(task["task_type"], "map")
            self.assertEqual(task["input_file"], input_files[i % len(input_files)])
            self.assertEqual(task["task_id"], i)
            self.assertEqual(task["num_reduce_tasks"], num_reduce_tasks)

        # Check reduce tasks
        for i in range(num_reduce_tasks):
            task = tasks.get()
            print("task: ", json.dumps(task).encode())
            self.assertEqual(task["task_type"], "reduce")
            self.assertEqual(task["bucket_id"], i)
            map_task_ids = [
                j for j in range(num_map_tasks) if j % num_reduce_tasks == i
            ]
            self.assertEqual(task["map_task_ids"], map_task_ids)
    

if __name__ == "__main__":
    unittest.main()
