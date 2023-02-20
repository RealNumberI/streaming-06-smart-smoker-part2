"""
    This program listens for work messages contiously.
    The smart_smoker_emitter.py must run to send the messages

    Name: Tanya Fagaly
    Date: 2/15/23
    
"""
### Lbrary Imports ###
import pika
import sys
import time
import csv
from collections import deque


# Create deques for smoker, Food A and Food B to contain the values, with limited lengths for each according to the directions:
# Deque Max Length
#    At one reading every 1/2 minute, the smoker deque max length is 5 (2.5 min * 1 reading/0.5 min)
smoker_deque = deque(maxlen=5)
#    At one reading every 1/2 minute, the food deque max length is 20 (10 min * 1 reading/0.5 min) 
food_a_deque = deque(maxlen=20)
food_b_deque = deque(maxlen=20)
# host defined
host = "localhost"

### Functions ###
# FUNCTION: Before starting again, delete previous queues
def delete_old_queue(host: str, old_queue: str):    
    """
    Because we will run multiple times, delete pervious runs of queue.
    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        old_queue (str): the name of the queue
    """
     # create a blocking connection to the RabbitMQ server
    conn = pika.BlockingConnection(pika.ConnectionParameters(host))
    # use the connection to create a communication channel
    ch = conn.channel()
    # use the channel to delete the queue
    ch.queue_delete(queue=old_queue)    
    # Run the delete queue function the queues
    

# FUNCTION: Receive messages 
def smoker_callback(ch, method, properties, body):
    """ Get a message about the smoker temperature."""
    # decode the binary message body to a string, append to a deque, and print that it has been received.
    smoker_message = body.decode()
    smoker_deque.append(smoker_message) 
    print(f" [x] Received Smoker {smoker_message}")
    # split the deque between temp and time and identify the first and last items in the deque.  Print them.
    smoker_deque_split = smoker_deque.split(",")
    smoker_first_temp = float(smoker_deque_split[0])
    smoker_last_temp = float(smoker_deque_split[-1])
    print("first: ", smoker_first_temp)
    print("last: ",smoker_last_temp)
    # Find the difference between the temps
    smoker_warning_temp = smoker_last_temp-smoker_first_temp
    # print temp or warning if it meets the criteria
    if smoker_warning_temp < -15:
        print("The smoker temperature decreased by more than 15 degrees F in 2.5 minutes (smoker alert!)")
    else:
        print("Smoker temp is ", smoker_last_temp)
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)       

# FUNCTION: Food A callback - defines warning
def food_a_callback(ch, method, properties, body):
    """ Define behavior on getting a message about the smoker temperature."""
    # decode the binary message body to a string
    food_a_message = body.decode()
    print(f" [x] Received Smoker {food_a_message}")
    # acknowledge the message was received and processed 

    # Add to deque and split temp and time
    food_a_unsplit_list= food_a_deque.append(food_a_message) 
    food_a_split_list = food_a_unsplit_list.split(", ")
    # Get first temp from deque which is the from the message 20 messages ago, since the whole list is limited to 20.
    food_a_first_temp = round(float(food_a_split_list[0]),1)
    # Get Food A last item
    food_a_last_temp = round(float(food_a_split_list[-1]),1)
    food_a_warning_temp = food_a_last_temp-food_a_first_temp
    # print temp or warning
    if food_a_warning_temp < 1:
        print("Food A temp change in temp is 1 F or less in 10 min (or 20 readings)  --> food stall alert!")
    else:
        print("Food A temp is ", food_a_last_temp)
     # acknowledge the message was received and processed 
     # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# FUNCTION: Food B callback - defines warning
def food_b_callback(ch, method, properties, body):
    """ Define behavior on getting a message about the smoker temperature."""
    # decode the binary message body to a string
    food_b_message = body.decode()
    print(f" [x] Received Smoker {food_b_message}")
    # acknowledge the message was received and processed 

    # Add to deque and split temp and time
    food_b_unsplit_list= food_a_deque.append(food_b_message) 
    food_b_split_list = food_b_unsplit_list.split(", ") 
    # Get first temp from deque which is the from the message 20 messages ago, since the whole list is limited to 20.
    food_b_first_temp = round(float(food_b_split_list[0]),1)
    # Get Food A last item
    food_b_last_temp = round(float(food_b_split_list[-1]),1)
    food_b_warning_temp = food_b_last_temp-food_b_first_temp
 # print temp or warning
    if food_b_warning_temp < 1:
        print("Food B temp change in temp is 1 F or less in 10 min (or 20 readings)  --> food stall alert!")
    else:
        print("Food B temp is ", food_b_last_temp)
    # acknowledge the message was received and processed  
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

# define a main function to run the program
def main(hn: str, qn1: str, qn2: str, qn3: str ):
    """ 
    Continuously listen for task messages on a named queue.
    Parameters:
    host (str): the host name or IP address of the RabbitMQ server
    smoker_temp_queue_name (str): the name of the smoker queue
    food_a_queue_name (str): the name of the food A queue
    food_b_queue_name (str): the name of the food B queue
    """   
    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()
        
        # do this once for each queue
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=qn1, durable=True)
        channel.queue_declare(queue=qn2, durable=True)
        channel.queue_declare(queue=qn3, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # do this once for each queue
        # configure the channel to listen on a specific queue,  
        # use the callback function for the associated queue/channel,
        # and auto-acknowledge the message  
        channel.basic_consume(queue=qn1, on_message_callback=smoker_callback, auto_ack=True)
        channel.basic_consume(queue=qn2, on_message_callback=food_a_callback, auto_ack=True)
        channel.basic_consume(queue=qn3, on_message_callback=food_b_callback, auto_ack=True)                

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function 
    main(host,'01-smoker','02-food-A','02-food-B')