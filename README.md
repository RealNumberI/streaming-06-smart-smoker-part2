# streaming-05-smart-smoker
### Tanya Fagaly
### February 4, 2023
https://github.com/RealNumberI/streaming-05-smart-smoker

> Use RabbitMQ to distribute tasks to multiple workers

One process will create task messages. Multiple worker processes will share the work. 


## Before You Begin

1. Fork this starter repo into your GitHub. **DONE**
1. Clone your repo down to your machine. **DONE**
1. View / Command Palette - then Python: Select Interpreter **DONE**
1. Select your conda environment. **DONE - I discovered I need to make sure my python version matches what I currently have to work. **

## Read

1. Read the [RabbitMQ Tutorial - Work Queues](https://www.rabbitmq.com/tutorials/tutorial-two-python.html)
1. 

## RabbitMQ Admin 

RabbitMQ comes with an admin panel. When you run the task emitter, reply y to open it. 

(Python makes it easy to open a web page - see the code to learn how.)

## Execute the Producer

1. Run emitter_of_tasks.py (say y to monitor RabbitMQ queues)

Explore the RabbitMQ website.

## Execute a Consumer / Worker

1. Run listening_worker.py

Will it terminate on its own? How do you know? **DONE  Yes, the first listener does.  It has a command line when completed**

## Ready for Work

1. Use your emitter_of_tasks to produce more task messages.

## Start Another Listening Worker 

1. Use your listening_worker.py script to launch a second worker. 

Follow the tutorial. 
Add multiple tasks (e.g. First message, Second message, etc.)
How are tasks distributed? 
Monitor the windows with at least two workers. 
Which worker gets which tasks?


## Reference

- [RabbitMQ Tutorial - Work Queues](https://www.rabbitmq.com/tutorials/tutorial-two-python.html)


## Screenshot

See a running example with at least 3 concurrent process windows here:
NOTE: In the listeners, in the first run of the emitter, I didn't start the listeners simultaneously. It was intersting to see where the second listener picked up as it started running. In the the second run, they were both already running.  

![Multiple terminals]( Screenshot-M0d4-HW.png )
