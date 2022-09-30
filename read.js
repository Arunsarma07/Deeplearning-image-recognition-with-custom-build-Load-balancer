var  AWS = require('aws-sdk');
var fs = require('fs');
const { resolve } = require('path');
const { exitCode } = require('process');

AWS.config.update({region: 'REGION'});
var sqs = new AWS.SQS({apiVersion: '2012-11-05'});

var queueURL = "https://sqs.us-east-1.amazonaws.com/317018765584/request_queue";
var responseQueueURL = "https://sqs.us-east-1.amazonaws.com/317018765584/response_queue";

//S3 config parameters
const s3 = new AWS.S3({
	accessKeyId: 'AKIAUTT6KPEIN7ZF4YPC',
	secretAccessKey: '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',
	region: "us-east-1",
	s3BucketEndpoint: false,
	endpoint: "https://s3.amazonaws.com"
});

var myresponse = "Image not found";
var img_name;

//Send the response of image classification to Response queue
let sendTheResponse = function(myresponse, image_name) {
	var params = {
		MessageAttributes: {
			"Title": {
				DataType: "String",
				StringValue: "Classification result"
			}
		},
		MessageBody: myresponse,
		QueueUrl: responseQueueURL
	}; 
	sqs.sendMessage(params, function(err, data) {
		if (err) {
			console.log("Error", err);
		} 
		else{
			console.log("Success", data.MessageId);
			console.log("Image Name: ", image_name);
			console.log("Classification result: ", myresponse); 
			let str = image_name.substring(0, image_name.length - 5);
			console.log(str);
			const params = {
				Bucket: 'outputbucketgroup13',
				Key: str,
				Body: myresponse
			};
			//Upload the classification result to S3 output Bucket
			s3.upload(params, function(err, data){
				if(err) {
					throw err;
				}
				console.log(`File uploaded successfull. ${data.Location}`);
			});

		}
	});
}

//Consumer queue to poll the Request queue to read the images
const {Consumer} = require('sqs-consumer');
const app = Consumer.create({
  queueUrl: queueURL,
  handleMessage: async (message) => {
    console.log('Image recieved')
	const obj = JSON.parse(message.Body);
	let buff = new Buffer(obj.data, 'base64');
	fs.writeFileSync(obj.filename, buff);
	img_name = obj.filename;
	
	const params = {
		Bucket: 'inputbucketgroup13',
		Key: img_name,
		Body: fs.readFileSync(img_name)
	};

	//uploading the images to S3 input bucket
	s3.upload(params, function(err, data){
		if(err) {
			throw err;
		}
		console.log(`File uploaded successfull. ${data.Location}`);
	});
		
	//Spawning the image classification function
	console.log("Spawning Python child");
	const { spawn, spawnSync } = require("child_process");
	console.log("Spawning the python....");
	const process = spawnSync('python3', ['/home/ubuntu/classifier/image_classification.py', img_name]);
	var responseToSend = process.stdout.toString()
	console.log(responseToSend);
	console.log(process.stderr.toString());
	sendTheResponse(responseToSend, img_name);
  },
  sqs: new AWS.SQS()
});

app.on('error', (err) => {
  console.log(err.message);
});

app.start();