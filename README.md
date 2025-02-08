# JK-tech-Task-Scheduler
  - I have used here the threading pool machenism in python with the help of threading concurrency and for that
    we have also used condition wait for theading.

  - I have made two classes for making this code modular
    1. Task - For creating a task
    2. TaskScheduler - For scheduling the tasks and managing the tasks

       
  - Code flow
    1. Designing the tasks in execution code block
    2. Creating TaskScheduler obj to access all its properties and features
    3. Adding task - It creats Task object and adds task objects to list where I am mapping dependencies of tasks.
    4. Sorting the dependencies with asending order using prebuild sorting method.
    5. Running task concurrently with max thread pool
      - In the there are some methods that perform some dependency mechanism and waiting methology for which task to wait and which to run
      - I used lock method of thread where threads will lock assigm completing status to itself.
      - notify method will update all thread that it has be completed a
      - 
      
