// noinspection JSUnresolvedFunction,NpmUsedModulesInstalled
let AWS = require('aws-sdk');
// noinspection ES6ShorthandObjectProperty,NpmUsedModulesInstalled
let {ElasticSearchService} = require('ElasticSearchService');
// noinspection ES6ShorthandObjectProperty,NpmUsedModulesInstalled
let {DynamoService} = require('DynamoService');

//noinspection JSUnresolvedFunction,JSUnresolvedVariable
let region = process.env.region || 'us-west-1';
AWS.config.update({region: region});


// noinspection JSUnresolvedFunction
let elasticSearchEndPoint = new AWS.Endpoint(process.env.elasticURL || '');
let elastic = new ElasticSearchService(AWS, region, elasticSearchEndPoint);

//noinspection JSUnresolvedFunction
let dynamoDB = new AWS.DynamoDB({apiVersion: '2012-08-10'});
let dynamo = new DynamoService(dynamoDB, process.env.metaTable || 'app-Meta', process.env.dataTable || 'app-Data');


let processRecord = function(item, callback){

    let record = item['dynamodb']['NewImage'] || null;
    let oldRecord = item['dynamodb']['OldImage'] || null;

    if (record !== null) {

        let docType = record['type']['S'];
        let newStructure = record['structure']['S'];
        let oldStructure = oldRecord !== null ? oldRecord['structure']['S'] : null;

        console.log('docType is: ' + docType);
        console.log('record received: ' + JSON.stringify(record));


        dynamo.getMeta(docType, function (err, meta) {

            console.log('meta from dynamo:', meta);

            if ( meta !== null && ( (oldRecord !== null && oldStructure !== newStructure) || oldRecord === null) ) {

                //calculate new index name
                let newIndexName = docType + "_index_1";
                if (meta.currentIndex !== '' && meta.currentIndex !== 'null') {
                    let number = parseInt(meta.currentIndex.substr(meta.currentIndex.lastIndexOf("_") + 1)) + 1;
                    newIndexName = docType + "_index_" + number;
                }
                newIndexName = String(newIndexName).toLowerCase();


                elastic.createIndex(newIndexName, docType, newStructure, function (err, elasticResult) {

                    console.log('Elastic Search Said:', elasticResult);
                    if (typeof elasticResult.error === 'undefined') {
                        if (meta.currentIndex === '' || meta.currentIndex === 'null') {

                            elastic.putAlias(docType + "_alias", '', newIndexName, function (err, aliasUpdateResponse) {
                                console.log('Alias Update Response:', aliasUpdateResponse);
                                if (typeof aliasUpdateResponse.error === 'undefined') {
                                    dynamo.putMeta(docType, newStructure, newIndexName, 'null', 'done', function (err, updateMetaResponse) {
                                        console.log('Update Meta Response', updateMetaResponse);
                                        if (callback !== null) callback(null, {message: "Created new index " + newIndexName});
                                    });
                                }
                            });

                        } else {

                            //@todo: change to promises - Feb 7 2018
                            dynamo.putMeta(docType, meta.structure, meta.currentIndex, newIndexName, 'migrating', function (err, updateMetaResponse) {
                                console.log('Update Meta Response', updateMetaResponse);
                                dynamo.indexData(docType, elastic, newIndexName, function (err, indexDataResponse) {
                                    console.log('Index Data Response', indexDataResponse);
                                    elastic.putAlias(docType + "_alias", meta.currentIndex, newIndexName, function (err, aliasUpdateResponse) {
                                        console.log('Alias Update Response', aliasUpdateResponse);
                                        dynamo.putMeta(docType, newStructure, newIndexName, 'null', 'done', function (err, updateMetaResponse) {
                                            console.log('Update Meta Response', updateMetaResponse);

                                            elastic.deleteIndex(meta.currentIndex, function (err, deleteIndexResponse) {
                                                console.log("Delete Index Response", deleteIndexResponse);
                                                if (callback !== null) callback(null, {message: "Created new index " + newIndexName});
                                            });


                                        });
                                    });
                                })
                            });

                        }
                    }
                });


            }
        });

    }
};



/** MAIN **/
module.exports.metaHandler = (event, context, callback) => {


    if (typeof event['Records'] !== 'undefined' && event['Records'].length > 0) {
        event['Records'].forEach(function (item) {

            console.log('Doing Record:', item);

            if (item['eventName'] !== 'REMOVE') {
                processRecord(item, function (err, result) {
                    callback(null, result);
                    //noinspection JSUnresolvedFunction
                    context.succeed(result);
                });
            } else {

                let oldRecord = item['dynamodb']['OldImage'] || null;
                let docType = oldRecord['type']['S'];
                let currentIndex = typeof oldRecord['currentIndex'] !== 'undefined' ? oldRecord['currentIndex']['S'] : '';
                if (currentIndex !== '') {
                    elastic.deleteAlias(docType + "_alias", currentIndex, function (err, deleteAliasResponse) {
                        console.log('Delete Alias Response', deleteAliasResponse);
                        elastic.deleteIndex(currentIndex, docType, function (err, deleteIndexResponse) {
                            console.log('Delete Index Response', deleteIndexResponse);
                        });
                    });
                }

            }

        });
    }



};

