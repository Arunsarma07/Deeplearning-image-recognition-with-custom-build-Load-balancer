#Importing the required modules
import flask
import werkzeug
import boto3
from botocore.exceptions import ClientError
from flask_caching import Cache
import time
import os
import math
import json
import base64
import logging
import threading
from queue import Queue

#Logger
logger = logging.getLogger(__name__)
#SQS resource from boto3 module
sqs = boto3.resource('sqs', region_name = 'us-east-1', aws_access_key_id ="AKIAUTT6KPEIN7ZF4YPC", aws_secret_access_key ="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")
#EC2 resource from boto3 module
ec2 = boto3.client('ec2', 'us-east-1', aws_access_key_id ="AKIAUTT6KPEIN7ZF4YPC", aws_secret_access_key ="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")
#Fetching the SQS queue name to read the response queue
sqs_queue = sqs.get_queue_by_name(QueueName ='response_queue')
q = Queue()

#Maximum number of app-tier insatnces
max_no_of_inst = 20

#Data structure to keep track of the instances
instance_info ={
    'total_instances' : 0,
    'running_instances' : 0,
    'running_instance_ids':[],
    'stopped_instances' : 0,
    'stopped_instance_ids':[]
}

config = {
    "DEBUG": True,          # some Flask specific configs
    "CACHE_TYPE": "SimpleCache",  # Flask-Caching related configs
    "CACHE_DEFAULT_TIMEOUT": 1000
}

#Send message function to send the images to request queue
def send_message(queue, message_body, message_attributes=None):
    if not message_attributes:
        message_attributes = {}

    try:
        response = queue.send_message(
            MessageBody=message_body,
            MessageAttributes=message_attributes
        )
    except ClientError as error:
        logger.exception("Send message failed: %s", message_body)
        raise error
    else:
        return response

#Flask app 
app = flask.Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def handle_request():
    #reading the image from the POST request
    if flask.request.method == 'POST':
        image = flask.request.files['myfile']
        filename = werkzeug.utils.secure_filename(image.filename)
        print("Received ", filename)

        #saving the image on local and converting the image to JSON format 
        path = '/home/ec2-user/webtier/'
        image.save(os.path.join(path, filename))
        data={}
        with open('/home/ec2-user/webtier/'+filename, mode='rb') as file:
            img = file.read()
        data['img'] = base64.encodebytes(img).decode('utf-8') #converting image to bytes
        data['name'] = filename
        msgbody = json.dumps(data)      #converting the image and file name to JSON format

        #Sending the JSON message to request queue
        queue = sqs.get_queue_by_name(QueueName ='request_queue')
        print("Sending the image to request queue..")
        resp = send_message(queue, msgbody)

        #deleting the file from the local file
        if os.path.exists(data['name']):
            os.remove(data['name'])
        print("Image sent successfully..\n\n")

    return "Sent to classification"

#process_message function to process the message body
def process_message(message_body):
    print(f"classification result: {message_body}")
    q.put(message_body)
    pass

#reading the response queue
def read_response_queue():
    while True:
        messages = sqs_queue.receive_messages()
        for message in messages:
            process_message(message.body)
            message.delete()

#Autoscaling functions

#fetching instance information fetches all the instance ids and their states and updates the data structure
def fetch_instance_information(instance_info):
    reservations = ec2.describe_instances(Filters=[
        {
            "Name": "instance-state-name",
            "Values": ["running", "pending", "shutting-down", "terminated", "stopping", "stopped"],
        }
    ]).get("Reservations")
    a = [{'Key': 'Name', 'Value': 'web-tier1'}]
    for reservation in reservations:
        for instance in reservation["Instances"]:
            if(a != instance["Tags"]):
                if ((instance["State"]["Name"] == "running") or (instance["State"]["Name"] == "pending")) and (instance["InstanceId"] not in instance_info['running_instance_ids']):
                    instance_info['running_instances']= instance_info['running_instances']+1
                    instance_info['running_instance_ids'].append(instance["InstanceId"])
                if ((instance["State"]["Name"] == "stopping") or (instance["State"]["Name"] == "stopped")) and (instance["InstanceId"] not in instance_info['stopped_instance_ids']):
                    instance_info['stopped_instances']= instance_info['stopped_instances']+1
                    instance_info['stopped_instance_ids'].append(instance["InstanceId"])
    
    instance_info['total_instances'] = instance_info['running_instances'] + instance_info['stopped_instances']

#create instances
def create_instance(instance_info):
    instances = ec2.run_instances(
        ImageId="ami-0cdcd6caa123f9ca4",
        MinCount=1,
        MaxCount=1,
        InstanceType="t2.micro",
        KeyName="arunsharma123",
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags' :[{
                'Key' : 'Name',
                'Value' : 'app-instance1'
            }]
        }]
    )
    instance_info['running_instances'] = instance_info['running_instances']+1
    instance_info['running_instance_ids'].append(instances["Instances"][0]["InstanceId"])
    instance_info['total_instances'] = instance_info['total_instances'] + 1

#terminate instances
def terminate_instance(instance_id):
    response = ec2.terminate_instances(InstanceIds=[instance_id])
    print("Terminated Instances: ", instance_id)

#start instances
def start_instance(instance_id):
    response = ec2.start_instances(InstanceIds =[instance_id])
    print("Started Instance: ", instance_id)

#stop instances
def stop_instance(instance_id):
    response = ec2.stop_instances(InstanceIds =[instance_id])
    print("Stopped Instance: ", instance_id)

#get sqs information
def fetch_queue_information():
    client = boto3.client('sqs',
    region_name = 'us-east-1', 
    aws_access_key_id ="AKIAUTT6KPEIN7ZF4YPC", 
    aws_secret_access_key ="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")

    response = client.get_queue_attributes(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/317018765584/request_queue',
        AttributeNames=['ApproximateNumberOfMessages']
    )
    return response['Attributes']['ApproximateNumberOfMessages']

#Auto-Scaling function
def auto_scaling():
    #Inside Autoscaling function
    print("\n\nAuto Scaling: ")

    #Fetching the Instance information and printing it
    fetch_instance_information(instance_info)
    print("Total number of instances: ", instance_info['total_instances'])
    print("Running Instances: ", instance_info['running_instances'])
    print("Stopped Instances: ", instance_info['stopped_instances'])

    #reading number of messages from queue
    noOfMsgs = fetch_queue_information()
    print("\nNo of messages in queue: ", noOfMsgs)

    #calculating number of instances required
    #assuming each instance can handle 3 requests
    required_instances = math.ceil(int(noOfMsgs) / 3)

    #maximum number of instances are limited to 20
    #if required instances are greater than 20, 
    #we set the require instances to 20
    if(required_instances > 20):
        required_instances = 20
    print("No of required instances: ", required_instances)
    
    #If required instances are greater than total instances,
    #then we create the shortage instances and start the remaining instances
    #and update the data structure
    if(required_instances > instance_info['total_instances']):
        create_num = required_instances - instance_info['total_instances']
        for i in range(create_num):
            create_instance(instance_info)
        for i in instance_info['stopped_instance_ids']:
            reservation = ec2.describe_instances(InstanceIds=[i]).get("Reservations")
            state =""
            for res in reservation:
                for inst in res["Instances"]:
                    state = inst["State"]["Name"]
                if state == "stopped":
                    start_instance(i)
                    instance_info['running_instance_ids'].append(i)
                    instance_info['stopped_instance_ids'].remove(i)
                    instance_info['running_instances'] = instance_info['running_instances'] + 1
                    instance_info['stopped_instances'] = instance_info['stopped_instances'] - 1
    
    #else if the required instances are lesser than the total instances and
    #greater than running instances, we start the remaining instances and update data structure
    elif (required_instances > instance_info['running_instances']):
        num_of_ins = required_instances - instance_info['running_instances']
        for i in instance_info['stopped_instance_ids']:
            reservation = ec2.describe_instances(InstanceIds=[i]).get("Reservations")
            state =""
            for res in reservation:
                for inst in res["Instances"]:
                    state = inst["State"]["Name"]
                if state == "stopped":
                    start_instance(i)
                    instance_info['running_instance_ids'].append(i)
                    instance_info['stopped_instance_ids'].remove(i)
                    instance_info['running_instances'] = instance_info['running_instances'] + 1
                    instance_info['stopped_instances'] = instance_info['stopped_instances'] - 1
                    num_of_ins = num_of_ins-1
            if(num_of_ins == 0):
                break
    
    #else if required instances are lesser than running instances,
    #then we stop the remaining instances and update the data structure
    elif (required_instances < instance_info['running_instances']):
        num_of_ins = instance_info['running_instances'] - required_instances
        for i in instance_info['running_instance_ids']:
            reservation = ec2.describe_instances(InstanceIds=[i]).get("Reservations")
            state =""
            for res in reservation:
                for inst in res["Instances"]:
                    state = inst["State"]["Name"]
                if state == "running":
                    stop_instance(i)
                    instance_info['stopped_instance_ids'].append(i)
                    instance_info['running_instance_ids'].remove(i)
                    instance_info['stopped_instances'] = instance_info['stopped_instances'] + 1
                    instance_info['running_instances'] = instance_info['running_instances'] - 1
                    num_of_ins = num_of_ins-1
            if(num_of_ins == 0):
                break
    
    #we call the thread timer after 10secs to autoscale the application
    threading.Timer(10, auto_scaling).start()


if __name__ == "__main__":
    t1 = threading.Thread(target=read_response_queue, name='t1')
    t1.start()
    auto_scaling()
    app.run(host="0.0.0.0", port=8080, debug=False)

