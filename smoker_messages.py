"""
    Author: Tanya Fagaly
    Date: February 10, 2023
    The python code below will retrieve a timestamp and three tempertures from  data file in csv format.
    The program will combine each temperature with a timestamp and send is as a message.
    The program uses the RabbitMQ server.
"""
## Libraries
import pika
import sys
import webbrowser
import csv
import time

## Define Functions

# FUNCTION: Show Rabbit MQ admin page if show_offer is True
def offer_rabbitmq_admin_site(show_offer):
    """
    Offer to open the RabbitMQ Admin website
    """
    if show_offer == True:
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

# FUNCTION: Creates and sends a message to the queue each execution.
def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.
    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """
    try:       
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()


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


def csv_processing(csv_file):
    """
Open the csv file, read it into tuples/list, send messages to tuples, send out messages
    """
    with open(csv_file, 'r') as file:
        reader = csv.reader(file, delimiter=",")      
        for row in reader:
            # Grab timestamp data 
            timestamp = f"{row[0]}"
            # Grab the smoker temperature data  
            Channel1 = f"{row[1]}"
            # Grab Food A temperature
            Channel2 = f"{row[2]}"
            # Grab Food B temperature
            Channel3 = f"{row[3]}"
	        # Make the message tuples  
            # Send a tuple of (timestamp, smoker temp) to the first queue 
            # Send a tuple of (timestamp, food A temp) to the second queue 
            # Send a tuple of (timestamp, food B temp) to the third queue 
            first_queue=f"{timestamp},{Channel1}"
            second_queue=f"{timestamp},{Channel2}"
            third_queue=f"{timestamp},{Channel3}"

            # Create a binary message from our tuples before using the channel to publish each of the 3 messages.
            smoker_message = first_queue.encode()
            food_a_message = second_queue.encode()
            food_b_message = third_queue.encode()

            # Send the messages using predefined function
            send_message (host, "01-smoker", smoker_message)
            send_message (host, "02-food-A", food_a_message)
            send_message (host, "03-food-B", food_b_message)
            # Slow messages
            time.sleep(30)


# Define variables
# Define RabbitMQ console offer and host
show_offer = False
host = "localhost"

# Define the csv data file 
data_file = 'smoker-temps.csv'

# Run first function: offer RabbitMQ console
offer_rabbitmq_admin_site

# Run second funtion: Delete the queue
delete_old_queue (host, "01-smoker")
delete_old_queue (host, "02-food-A")
delete_old_queue (host, "03-food-B")

# Run the csv file processing function
csv_processing(data_file)
