/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */
var deagg = require('aws-kinesis-agg/kpl-deagg');
var async = require('async');
require('constants');
var computeChecksums = true;

var ok = 'OK';
var error = 'ERROR';

/** function which closes the context correctly based on status and message */
var finish = function(event, context, status, message) {
	"use strict";

	console.log("Processing Complete");

	// log the event if we've failed
	if (status !== ok) {
		if (message) {
			console.log(message);
		}

		// ensure that Lambda doesn't checkpoint to kinesis
		context.done(status, JSON.stringify(message));
	} else {
		context.done(null, message);
	}
};

/** function which handles cases where the input message is malformed */
var handleNoProcess = function(event, callback) {
	"use strict";

	var noProcessReason;

	if (!event.Records || event.Records.length === 0) {
		noProcessReason = "Event contains no Data";
	}
	if (event.Records[0].eventSource !== "aws:kinesis") {
		noProcessReason = "Invalid Event Source " + event.eventSource;
	}
	if (event.Records[0].kinesis.kinesisSchemaVersion !== "1.0") {
		noProcessReason = "Unsupported Event Schema Version " + event.Records[0].kinesis.kinesisSchemaVersion;
	}

	if (noProcessReason) {
		finish(event, error, noProcessReason);
	} else {
		callback();
	}
};


/**
 * Example lambda function which uses the KPL syncronous deaggregation interface
 * to process Kinesis Records from the Event Source
 */
exports.process = function(event, context ,recordProcessor,aggregatedRecordsHandler) {
	"use strict";

	

	handleNoProcess(event, function() {
		//console.log("Processing " + event.Records.length + " Kinesis Input Records");
		var totalUserRecords = 0;

		async.map(event.Records, function(record, asyncCallback) {
			// use the deaggregateSync interface which receives a single
			// callback with an error and an array of Kinesis Records
			deagg.deaggregateSync(record.kinesis, computeChecksums, function(err, userRecords) {
				if (err) {
					console.log(err);
					asyncCallback(err);
				} else {
					//console.log("Received " + userRecords.length + " Kinesis User Records");
					totalUserRecords += userRecords.length;
					var processedRecords=[];
					userRecords.map(function(record) {
						var recordDataBuffer = new Buffer(record.data, 'base64');
						var recordDataString=recordDataBuffer.toString('ascii')	
					//	console.log("Kinesis Aggregated User Record:" + recordDataString);
						var recordDataObj=JSON.parse(recordDataString);
					//	console.log("Kinesis Aggregated User Record Object:" +JSON.stringify(recordDataObj, null, 2) );

						var processedRecord=recordProcessor(recordDataObj);
						processedRecords.push(processedRecord);

						if(aggregatedRecordsHandler!=null && aggregatedRecordsHandler!=undefined){
							aggregatedRecordsHandler(processedRecords);
						}
						
						

						// you can do something else with each kinesis
						// user record here!
					});

					// call the async callback to reflect that the kinesis
					// message is completed
					asyncCallback(err);
				}
			});
		}, function(err, results) {
			// function is called once all kinesis records have been processed
			// by async.map
			if (debug) {
				console.log("Completed processing " + totalUserRecords + " Kinesis User Records");
			}

			if (err) {
				finish(event, context, error, err);
			} else {
				finish(event, context, ok, "Success");
			}
		});
	});
};