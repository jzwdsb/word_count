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
            self.assertEqual(task["input_files"], input_files[i : i + 1])
            self.assertEqual(task["task_id"], i)
            self.assertEqual(task["num_reduce_tasks"], num_reduce_tasks)

        # Check reduce tasks
        for i in range(num_reduce_tasks):
            task = tasks.get()
            print("task: ", json.dumps(task).encode())
            self.assertEqual(task["task_type"], "reduce")
            self.assertEqual(task["bucket_id"], i)
            self.assertEqual(task["map_task_ids"], [i for i in range(num_map_tasks)])

    def test_map_task(self):
        test_file_path = "inputs/pg-sherlock_holmes.txt"
        with open(test_file_path, "r") as test_file:
            test_file_contents = re.findall(r"\w+", test_file.read())
            map_task(test_file_path, 0, 1)
            # Check that the intermediate file was created
            self.assertTrue(os.path.exists("intermediate/mr-0-0"))
            # Check that the intermediate file has the correct number of words
            with open("intermediate/mr-0-0", "r") as f:
                words = f.read().splitlines()
                self.assertEqual(len(words), len(test_file_contents))

    def test_reduce_task(self):
        test_file_path = "inputs/pg-sherlock_holmes.txt"
        counts = collections.Counter()
        with open(test_file_path, "r") as test_file:
            words = re.findall(r"\w+", test_file.read())
            counts.update(words)
        with open("intermediate/mr-0-0", "w") as f:
            f.write("\n".join(words))
        reduce_task(0, [0])
        # Check that the output file was created
        self.assertTrue(os.path.exists("out/out-0"))
        # Check that the output file has the correct number of words
        with open("out/out-0", "r") as f:
            words = f.read().splitlines()
            self.assertEqual(len(words), len(counts))
            for word in words:
                self.assertEqual(counts[word.split(" ")[0]], int(word.split(" ")[1]))


if __name__ == "__main__":
    unittest.main()
