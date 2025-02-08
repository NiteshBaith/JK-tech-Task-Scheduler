
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from typing import List, Dict
from functools import cmp_to_key

class Task:
    """Represents a task with a unique ID, execution duration, dependencies, and priority."""
    
    def __init__(self, task_id: str, processing_time: int, dependencies: List[str], priority: int):
        self.task_id = task_id
        self.processing_time = processing_time
        self.dependencies = dependencies
        self.priority = priority

    def __repr__(self):
        return f"Task({self.task_id}, {self.priority}, {self.dependencies})"


class TaskScheduler:
    """Concurrent Task Scheduler securing task dependencies and priority-based execution."""
    
    def __init__(self, max_concurrent: int):
        self.max_concurrent = max_concurrent
        self.tasks: Dict[str, Task] = {} # Stores task objects by ID
        self.dependency_graph = defaultdict(set) # Task dependencies mapping
        self.task_completion_status = {} # Tracks completed tasks
        self.lock = threading.Lock() # Ensures thread-safe task completion tracking
        self.condition = threading.Condition(self.lock) # Used for dependency synchronization
        self.completed_tasks = 0 # Atomic counter for completed tasks

    def add_tasks(self, task_list: List[Dict]):
        """Adds tasks to the scheduler."""
        for task in task_list:
            task_obj = Task(task["id"], task["processingTime"], task["dependencies"], task["priority"])

            self.tasks[task_obj.task_id] = task_obj
            self.task_completion_status[task_obj.task_id] = False # Initially, no task is completed so marked status false
        
            
            # Building dependency graph
            
            for dep in task_obj.dependencies:
                self.dependency_graph[task_obj.task_id].add(dep)

    def _task_priority_comparator(self, task1, task2) -> int:
        """Custom comparator for sorting tasks by priority (lower values mean higher priority)."""
        if task1.priority != task2.priority:
            return task1.priority - task2.priority
        return 0

    def _wait_for_dependencies(self, task: Task):
        """Waits until all dependencies of a task are completed."""
        with self.condition:
            while any(not self.task_completion_status[dep] for dep in task.dependencies):
                self.condition.wait()

    def _execute_task(self, task: Task): 
        
        """Executes a given task, ensuring dependencies are met and updating completion status."""
        self._wait_for_dependencies(task) # Ensure dependencies are resolved
        
        print(f"Task {task.task_id} started. (Priority: {task.priority})")
        time.sleep(task.processing_time) # Simulate execution time
        print(f"Task {task.task_id} completed.")


        with self.lock:
            self.task_completion_status[task.task_id] = True
            self.completed_tasks += 1
            print(f"Tasks Completed: {self.completed_tasks}")
            self.condition.notify_all() # Notify waiting threads that a task is completed

    def run(self):
        """Executes tasks in parallel while respecting dependencies and priorities."""
        task_list = list(self.tasks.values())
        
        # Sort tasks by priority (ascending order means higher priority first)
        task_list.sort(key=cmp_to_key(self._task_priority_comparator))
        # print(task_list)
        
        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            futures = {executor.submit(self._execute_task, task): task for task in task_list}
            for future in futures:
                future.result() # Ensures all tasks complete before exiting

# Example usage
if __name__ == "__main__":
    tasks = [
        {"id": "task1", "processingTime": 2, "dependencies": [], "priority": 1},

        {"id": "task2", "processingTime": 1, "dependencies": ["task1"], "priority": 2},

        {"id": "task3", "processingTime": 3, "dependencies": ["task1"], "priority": 1},

        {"id": "task4", "processingTime": 1, "dependencies": ["task2", "task3"], "priority": 3},

        {"id": "task5", "processingTime": 2, "dependencies": ["task4"], "priority": 2},

        {"id": "task6", "processingTime": 2, "dependencies": ["task5"], "priority": 1},

        {"id": "task7", "processingTime": 1, "dependencies": ["task5"], "priority": 3},

        {"id": "task8", "processingTime": 2, "dependencies": ["task5"], "priority": 2}
    ]

    scheduler = TaskScheduler(max_concurrent=5)
    scheduler.add_tasks(tasks)
    scheduler.run()

