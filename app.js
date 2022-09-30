//using express and multer for the requests
const express =  require("express");
const multer = require("multer");
const AWS = require('aws-sdk');
const app = express();
const port = 1234;		//app is hosted on the 3000 port
const fs = require('fs');
var start_time = +new Date();

const upload = multer({dest: __dirname + '/uploaded_images'});
var ec2 = new AWS.EC2({apiVersion: '2016-11-15'});

var credentials = new AWS.SharedIniFileCredentials({profile: 'default'});
AWS.config.credentials = credentials;
AWS.config.update({region: 'us-east-1'});
var sqs = new AWS.SQS({apiVersion: '2012-11-05'})

var num_requests = 0;
var num_responses = 0;
const max_num_instances = 20;
var queue = 0;

global.totalInstances = 0;
global.runningInstances = 0;
global.runningInstancesIds = [];
global.stoppedInstances = 0;
global.stoppedInstancesIds = [];

if (fs.existsSync('responses.txt')){
	fs.unlinkSync('responses.txt')
}

//Reads the Post request that comes at port 3000
app.post('/', upload.single('myfile'), function(request, response){
	console.log("Recieved a post message");
	if(request.file) console.log("Filename: ", request.file.originalname);
	var fs = require('fs');
	fs.rename(__dirname+'/uploaded_images/' + request.file.filename, __dirname+'/uploaded_images/' + request.file.originalname, function(err){
		if(err) console.log('Error: ' + err);
	});
	const buff = fs.readFileSync('./uploaded_images/' + request.file.originalname);
	const base64buff = buff.toString('base64');
	var jsonBody = {
		filename : request.file.originalname,
		data: base64buff
	};
	var params = {
		MessageBody: JSON.stringify(jsonBody),
		QueueUrl: "https://sqs.us-east-1.amazonaws.com/317018765584/request_queue" 
	}; 
	console.log("Sending the image to request queue..!!")
	var sqsResponse = sqs.sendMessage(params).promise();
	sqsResponse.then(() => {
  		response.send(request.file.originalname + ' uploaded!');
  		num_requests += 1
		console.log("Number of requests recieved: ", num_requests);
  	}).catch((err) => {
  		response.send(request.file.originalname+' uploaded!');
  	})

});

//creating EC2 Instances
function create_ec2_instance(totalInstances, runningInstances, runningInstancesIds, resolve) {
	console.log("\nCreating the instances..\n");
	var commandString = `#!/bin/bash
				node /home/ubuntu/app-tier/read.js >> /home/ubuntu/app-tier/output.txt`;
	var instanceParams = {
		ImageId: 'ami-0f236aff7a45363aa',
		InstanceType: 't2.micro',
		SecurityGroupIds : ['sg-084912bb872e72317'],
		KeyName: 'mykeypair',
		MinCount: 1,
		MaxCount: 1,
		UserData: new Buffer(commandString).toString('base64')
	};
	var ec2 = new AWS.EC2({apiVersion: '2016-11-15'});
	var instancePromise = ec2.runInstances(instanceParams).promise();

	instancePromise.then(
	function(data) {
		data.Instances.forEach(function(instance){
			var id = instance.InstanceId;
			
			totalInstances = totalInstances + 1;
			
			runningInstances = runningInstances + 1;
			console.log("\n(create func data)running instances: ", runningInstances);
			console.log("(create func data)total instance: ", totalInstances);
			console.log("(create func data)Created instance: ", id);
			console.log(id);
			runningInstancesIds.push(id);
			tagParams = {Resources: [id], Tags: [
				{
				Key: 'Name',
				Value: 'app-tier-code'
				}
			]};
			var tagPromise = new AWS.EC2({apiVersion: '2016-11-15'}).createTags(tagParams).promise();
				tagPromise.then(function(data) {
					console.log("(create func data)Instance tagged\n");
				}).catch(function(err) {
					console.error(err, err.stack);
				});
			});
		})
  return resolve();
}

//Starting EC2 instances
function start_instance(runningInstances, runningInstancesIds, stoppedInstances, stoppedInstancesIds, resolve) {
	console.log("\nStarting the instances..\n");
	if(stoppedInstancesIds.length == 0){
		return resolve();
	}
	var ec2 = new AWS.EC2({apiVersion: '2016-11-15'});
	var params = {
		InstanceIds: stoppedInstancesIds,
		DryRun: true
	  };
	ec2.startInstances(params, function(err, data) {
		if (err && err.code === 'DryRunOperation') {
		  params.DryRun = false;
		  ec2.startInstances(params, function(err, data) {
			  if (err) {
				console.log("Error", err);
			  } else if (data) {
				//console.log("Success", data.StartingInstances);
				data.StartingInstances.forEach(function(instance){
					var id = instance.InstanceId;
					runningInstances+=1;
					stoppedInstances-=1;
					runningInstancesIds.push(id);
					var i = stoppedInstancesIds.indexOf(id);
					stoppedInstancesIds.splice(i, 1);
					console.log("\n(start data)running instances: ", runningInstances);
					console.log("(start data)total instance: ", totalInstances);
					console.log("(start data)Stopped Instances: ", stoppedInstances);
				})
			  }
			  return resolve();
		  });
		} else {
		  console.log(err);
		}
	});
}

//Stop instances
function stop_instance(runningInstances, runningInstancesIds, stoppedInstances, stoppedInstancesIds, resolve) {
	console.log("\nStopping the instances..\n");
	var ec2 = new AWS.EC2({apiVersion: '2016-11-15'});
	console.log("Stopping instances:");
	if(runningInstancesIds.length ==0){
		return resolve();
	}
	var params = {
		InstanceIds: runningInstancesIds,
		DryRun: true
	};
	ec2.stopInstances(params, function(err, data) {
		if (err && err.code === 'DryRunOperation') {
		  params.DryRun = false;
		  ec2.stopInstances(params, function(err, data) {
			  if (err) {
				console.log("Error", err);
			  } else if (data) {
				console.log("Success", data.StoppingInstances);
				data.StoppingInstances.forEach(function(instance){
					var id = instance.InstanceId;
					runningInstances = runningInstances - 1;
					stoppedInstances = stoppedInstances + 1;
					var i = runningInstancesIds.indexOf(i);
					runningInstancesIds.splice(i, 1);
					stoppedInstancesIds.push(id);
					console.log("\n(stop data)running instances: ", runningInstances);
					console.log("(stop data)total instance: ", totalInstances);
					console.log("(stop data)Stopped Instances: ", stoppedInstances);
				});
			  }
			  return resolve();
		  });
		} else {
		  console.log(err);
		}
	  });
}

function fetch_instance_information(resolve, totalInstances, runningInstances, runningInstancesIds, stoppedInstances, stoppedInstancesIds) {
	console.log("\nFetching the Instance information: \n");
	var params = {
		DryRun: false
	};
	var ec2 = new AWS.EC2({apiVersion: '2016-11-15'});
	ec2.describeInstances(params, function(err, data) {
		if (err) {
		  console.log("Error", err.stack);
		} else {
		  var reservations = data.Reservations
		  reservations.forEach(function(reservation){
			instances = reservation.Instances
			instances.forEach(function(instance){
				var state = instance.State.Name
	    		var tagName = ''
	    		if(instance.Tags.length){
	    			tagName = instance.Tags[0].Value
	    		}
				if(tagName != 'web-tier'){
					//console.log('State: ' + state);
					if(state == 'running' || state == 'pending'){
						console.log('Instance Id: ' + instance.InstanceId + ' State: ' + state);
						runningInstances = runningInstances + 1;
						runningInstancesIds.push(instance.InstanceId);
						console.log("\n(fetch info)running instances: ", runningInstances);
					}else if(state == 'stopped' || state == 'stopping'){
						console.log('Instance Id: ' + instance.InstanceId + ' State: ' + state);
						stoppedInstances = stoppedInstances + 1;
						console.log("(fetch data)stopped instances: ", stoppedInstances);
						stoppedInstancesIds.push(instance.InstanceId);
					}
				}
			})
		  });
		  totalInstances = runningInstances + stoppedInstances;
		  console.log("(fetch data)total instances: ", totalInstances);
		}
	  });
	  return resolve();
}

function fetch_request_queue_info(queue, resolve){
	console.log("\nFetching the queue information...");
	var params = {
		QueueUrl: 'https://sqs.us-east-1.amazonaws.com/317018765584/request_queue',
		AttributeNames : ['ApproximateNumberOfMessages'],
	};
	sqs.getQueueAttributes(params, function(err, data){
	  	if (err) {
			console.log("Error", err);
		} else {
			queue = parseInt(data.Attributes.ApproximateNumberOfMessages);
			console.log("No of requests in queue: ", queue);
			return resolve(queue);
		}
	});
}

function auto_scaling_ec2(){	
	
	var fetch_promise = new Promise((fetch_promise) => {
		fetch_instance_information(fetch_promise, totalInstances, runningInstances, runningInstancesIds, stoppedInstances, stoppedInstancesIds);
	});

	fetch_promise.then(() => {
		console.log("Total Instances: ", totalInstances);
		console.log("Running Instances: ", runningInstances);
		console.log("Stopped Instances: ", stoppedInstances);

		var autoScaleProm = new Promise((resolve) => {
			
			var queuePromise = new Promise((resolve1) => fetch_request_queue_info(queue, resolve1));	

			queuePromise.then((resolve1) => {
				var requiredInstances = Math.ceil(resolve1/3);
				console.log(requiredInstances);
				if(requiredInstances >= max_num_instances){
					requiredInstances = max_num_instances;
				}

				if(requiredInstances > totalInstances){
					
					var createInstancePromise = new Promise ((createInstanceResolve) => {
						create_ec2_instance(totalInstances, runningInstances, runningInstancesIds, createInstanceResolve);
					})
					var startInstancePromise = new Promise((startInstanceResolve) => {
						start_instance(runningInstances, runningInstancesIds, stoppedInstances, stoppedInstancesIds, startInstanceResolve);
					})
					Promise.all([createInstancePromise, startInstancePromise]).then(() => {
						return resolve();
					})

				}
				else if(requiredInstances > runningInstances){
					var remaining = requiredInstances - runningInstances;
					start_instance(runningInstances, runningInstancesIds, stoppedInstances, stoppedInstancesIds.slice(0, remaining+1), commandonStart, resolve);

				}
				else if(requiredInstances < runningInstances){
					var remaining = runningInstances - requiredInstances;
					stop_instance(runningInstances, runningInstancesIds, stoppedInstances, stoppedInstancesIds.slice(0, remaining+1), resolve);
				}
				else{
					resolve(); 
				}

			});
		});
		autoScaleProm.then(() => {
			console.log("Calling again");
			setTimeout(auto_scaling_ec2, 5000);
		})
	});
	
}

//Consumer queue to poll the response queue
const { Consumer } = require('sqs-consumer');
const { resolve } = require("path");
const { Console } = require("console");

const consumer = Consumer.create({
  queueUrl: 'https://sqs.us-east-1.amazonaws.com/317018765584/response_queue',
  handleMessage: async (message) => {
	return new Promise((resolve) => {
    	console.log("Message body: ", message.Body)
    	var line = message.Body + "\n";
    	try{
    		fs.appendFileSync('responses.txt', line);
    		num_responses += 1
    		if(num_responses == num_requests){
    			var end_time = +new Date();
    			var elapsed_time = end_time-start_time;
    			var time_in_ms = elapsed_time.toString() + ' ms';
				console.log("Elapsed time: ", time_in_ms)
    		}
    	}catch(err){
    		console.log(err);
    	}
    	resolve();
    });
  },
  sqs: new AWS.SQS()
});

consumer.on('error', (err) => {
  console.error(err.message);
});

consumer.on('processing_error', (err) => {
  console.error(err.message);
});

consumer.start();

const hostname = '0.0.0.0';
app.listen(port, hostname, () => {
    console.log(`Server running at port ${port}`);
	//setTimeout(auto_scaling_ec2, 10000);
})


