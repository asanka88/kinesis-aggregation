var AWS = require('aws-sdk');
var proxy = require('proxy-agent');
var libPath = "./node_modules/aws-kinesis-agg"
var RecordAggregator = require(libPath + '/RecordAggregator');
require(libPath + "/constants");

var jsonRecordProcessor = require('./JsonRecordProcessor');
AWS.config.update({

  region: "us-east-1",
  // endpoint: "http://localhost:8000",
  httpOptions: { agent: proxy('http://http.proxy.fmr.com:8000') }
});
function printObj(obj) {
    var string = JSON.stringify(obj, null, 2);
    console.log(string);
}

var kinesisClient = new AWS.Kinesis({
    'region': 'us-east-1'
});


var i = 0;
var recordProcessor = function (record) {
    var streamName = "mm_processed_1";

    //        console.log("Processing inside date processor "+JSON.stringify(record, null, 2));
    var date = new Date(record.txnTimeStamp);
    var year = date.getFullYear();
    var month = date.getMonth() + 1;
    var date_ = date.getDate();
    var fullDate = year + "-" + month + "-" + date_;
    record.txnDate = fullDate;
    //printObj(record);
    var stringObj = JSON.stringify(record, null, 2);
    var params = {
        Records: [{
            Data: new Buffer(stringObj), /* required */
            PartitionKey: 'mm_data', /* required */
            ExplicitHashKey: 'mm_data_1'
        }

        ],
        StreamName: streamName /* required */

    };

    try {
       
        kinesisClient.putRecords(params, function (err, data) {
            console.log("==inside put record==")
            if (err) {
                console.log(err, err.stack); // an error occurred
            } else {
                console.log(data);
            }              // successful response
        });
    } catch (err) {
        console.error(err);
    }




    console.log("===end of processing===");


};


exports.processDate = function (event, context) {
    "use strict";

    jsonRecordProcessor.process(event, context, recordProcessor, null);

};