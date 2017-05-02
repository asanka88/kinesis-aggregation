/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

var deaggregator = require('./sample-deaggregation');
var libPath = "./node_modules/aws-kinesis-agg"
//var aggregator = require(libPath + '/RecordAggregator');
require(libPath + "/constants");
var dateProcessor=require('./DateProcessor')
// sample kinesis event - record 0 has no data, and will be filled in with
// dynamic content using protobuf
var event = {
  "Records": [
    {
      "kinesis": {
        "kinesisSchemaVersion": "1.0",
        "partitionKey": "a",
        "sequenceNumber": "49572815560010421343297161792929245786962888214485925890",
        "data": "84mawgoHbW1fZGF0YRp4CAAadHsidHhuVGltZVN0YW1wIjoxNDkzNjY4MDIyMTEyLCJhbW91bnQiOjM1MC4wLCJ0eG5UeXBlIjoiam91cm5hbCIsImFncmVlbWVudFR5cGUiOiJkaXN0cmlidXRpb24iLCJjaGFubmVsIjoiZmlkLmNvbSJ9GngIABp0eyJ0eG5UaW1lU3RhbXAiOjE0OTM2NjgwMjI4OTAsImFtb3VudCI6MzUwLjAsInR4blR5cGUiOiJqb3VybmFsIiwiYWdyZWVtZW50VHlwZSI6ImRpc3RyaWJ1dGlvbiIsImNoYW5uZWwiOiJmaWQuY29tIn2wO6mqiRlfnCZrPNG2vNwe",
        "approximateArrivalTimestamp": 1493668037.182
      },
      "eventSource": "aws:kinesis",
      "eventVersion": "1.0",
      "eventID": "shardId-000000000000:49572815560010421343297161792929245786962888214485925890",
      "eventName": "aws:kinesis:record",
      "invokeIdentityArn": "arn:aws:iam::067486486397:role/asankalambdakenesisdynamo",
      "awsRegion": "us-east-1",
      "eventSourceARN": "arn:aws:kinesis:us-east-1:067486486397:stream/mm_2"
    }
  ]
};

/** mock context object to simulate AWS Lambda context */
function context() {
}
context.done = function(status, message) {
	console.log("Context Closure Message - Status:" + JSON.stringify(status)
			+ " Message:" + JSON.stringify(message));

	if (status && status !== null) {
		process.exit(-1);
	} else {
		process.exit(0);
	}
};
context.getRemainingTimeInMillis = function() {
	return 60000;
};

try {
	// use the message aggregator to generated an encoded value
	// aggregator.aggregate(rawRecords, function(err, encoded) {
	// 	// bind the encoded value into the existing kinesis record set
	// 	event.Records[0].kinesis.data = encoded;

	// 	// use the deaggregator to extract the data
	// 	deaggregator.exampleAsync(event, context);
	// });
	dateProcessor.processDate(event,context);


} catch (e) {
	console.log(e);
	console.log(JSON.stringify(e));
	context.done(e);
}