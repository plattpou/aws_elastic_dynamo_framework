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
    let data = {
        id : record['id']['S'],
        type: record['type']['S'],
        content : record['content']['S']
    };

    if (record !== null) {
        dynamo.getMeta(data.type, function (err, meta) {

            let promises = [];
            ['currentIndex','nextIndex'].forEach(function (idx) {
                let index = meta[idx] || '';
                if (index !== '' && index !== 'null') {
                    let method = event === 'REMOVE' ? 'DELETE' : 'PUT';

                    promises.push(new Promise(function (resolve) {

                        console.log("Sending " + method + " to ES index " + index);
                        if (method === 'DELETE') {
                            elastic.deleteDocument(index, data, function (err, response) {
                                resolve({"err": err, "response": response});
                            });
                        } else {
                            elastic.putDocument(index, data, function (err, response) {
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
module.exports.dataHandler = (event, context, callback) => {


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