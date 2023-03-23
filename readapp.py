#importing required libraries
import boto3
import json
import base64
from PIL import Image
import io
import subprocess
from botocore.exceptions import ClientError
import logging
import os

#SQS resource
sqs = boto3.resource('sqs', region_name = 'us-east-1', aws_access_key_id ="XXXXXXXXXXXXX", aws_secret_access_key ="XXXXXXXXXXXXX")
queue = sqs.get_queue_by_name(QueueName="request_queue")
logger = logging.getLogger(__name__)
#S3 Client
s3client = boto3.resource('s3')

#S3 buckets to store images and output
input_bucket = "inputbucketproj1grp13"
output_bucket = "outputbucketproj1grp13"

#Sending message to response queue
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

#process the message received from the request queue
def process_message(message_body):
    #read the message from the request queue
    data = json.loads(message_body)
    img = Image.open(io.BytesIO(base64.decodebytes(bytes(data['img'], "utf-8"))))

    #saving the image to local folder
    img.save('/home/ubuntu/classifier/' + data['name'])
    print("Image saved ", data['name'])

    #Uploading the image to input bucket
    s3client.Bucket(input_bucket).upload_file('/home/ubuntu/classifier/'+data['name'], data['name'])

    #spawning the child process from the command
    cmd = f"/usr/bin/python3 /home/ubuntu/classifier/image_classification.py {'/home/ubuntu/classifier/'+data['name']}"
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    output = process.communicate()[0].decode("utf-8")
    print("Output: ", output)

    #sending the output to response queue
    queue = sqs.get_queue_by_name(QueueName ='response_queue')
    resp = send_message(queue, output)
    print("Response sent successfully..")

    #Sending the output to the output bucket
    name = str(data['name']).split(".")
    s3client.Bucket(output_bucket).upload_fileobj(io.BytesIO(output.encode("utf-8")), name[0])
    print("Sending the output to queue..\n")

    #deleting the image file from the local folder
    if os.path.exists('/home/ubuntu/classifier/'+data['name']):
        os.remove('/home/ubuntu/classifier/'+data['name'])
    pass

if __name__ == "__main__":
    while True:
        messages = queue.receive_messages()
        for message in messages:
            process_message(message.body)
            message.delete()
