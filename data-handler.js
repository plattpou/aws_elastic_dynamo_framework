// noinspection JSUnresolvedFunction,NpmUsedModulesInstalled
let AWS = require('aws-sdk');
// noinspection ES6ShorthandObjectProperty,NpmUsedModulesInstalled,JSUnresolvedFunction
let {ElasticSearchService} = require('ElasticSearchService');
// noinspection ES6ShorthandObjectProperty,NpmUsedModulesInstalled,JSUnresolvedFunction
let {DynamoService} = require('DynamoService');

//noinspection JSUnresolvedFunction,JSUnresolvedVariable
let region = process.env.region || 'us-west-1';
// noinspection JSUnresolvedVariable
AWS.config.update({region: region});


// noinspection JSUnresolvedFunction,JSUnresolvedVariable
let elasticSearchEndPoint = new AWS.Endpoint(process.env.elasticURL || '');
let elastic = new ElasticSearchService(AWS, region, elasticSearchEndPoint);

//noinspection JSUnresolvedFunction,JSUnresolvedVariable
let dynamo = new DynamoService(AWS, process.env.metaTable || 'app-Meta', process.env.dataTable || 'app-Data');


let processRecord = function(event, item, callback){

    let record = item['dynamodb']['NewImage'] || item['dynamodb']['OldImage'] || null;
    let docType = record['type']['S'];

    if (record !== null) {
        dynamo.getMeta(docType, function (err, meta) {

            let promises = [];
            ['currentIndex','nextIndex'].forEach(function (idx) {
                if ((meta[idx] || '') !== '') {
                    let method = event === 'REMOVE' ? 'DELETE' : 'PUT';

                    promises.push(new Promise(function (resolve) {

                        console.log("Sending " + method + " to ES index " + meta[idx]);
                        if (method === 'DELETE') {
                            elastic.deleteDocument(meta[idx], record, function (err, response) {
                                resolve({"err": err, "response": response});
                            });
                        } else {
                            elastic.putDocument(meta[idx], record, function (err, response) {
                                resolve({"err": err, "response": response});
                            });
                        }

                    }));

                    Promise.all(promises).then(function (results) {
                        console.log("Streamed Docs Result",results);
                        callback(results);
                    });
                }
            });

        });
    }
};

// noinspection JSUnresolvedVariable
/** MAIN **/
module.exports.metaHandler = (event, context, callback) => {


    if (typeof event['Records'] !== 'undefined' && event['Records'].length > 0) {
        event['Records'].forEach(function (item) {

            console.log('Streaming Record:', item);
            processRecord(item['eventName'], item, function (err, result) {
                callback(null, result);
                //noinspection JSUnresolvedFunction
                context.succeed(result);
            });

        });
    }


};