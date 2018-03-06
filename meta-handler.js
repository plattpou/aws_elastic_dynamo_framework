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

// noinspection JSMethodCanBeStatic
let indexData = function(docType, elastic, newIndexName, callback) {

    dynamo.getAllData(docType,function(err, page){
        console.log('pagination page',err, page);

        //noinspection JSUnresolvedVariable
        if (typeof page.Items !== 'undefined' && page.Items.length > 0) {

            let promises = [];
            // noinspection JSUnresolvedVariable
            page.Items.forEach(function(item){
                console.log('item:', item);
                promises.push(new Promise(function(resolve){
                    elastic.putDocument(newIndexName,item,function(err,result){
                        if (err) {
                            console.log('Error putting document in ' + newIndexName, err);
                        } else {
                            console.log('Indexed to ' + newIndexName, result);
                        }
                        resolve(result);
                    });
                }));
            });

            // noinspection JSUnresolvedFunction
            Promise.all(promises).then(function (result) {
                console.log('Finished Pagination Items', page,result);
                if (typeof page.finished !== 'undefined' && page.finished === true) {
                    console.log("FINISHED RE-INDEXING");
                    callback(null,{"message":"done re-indexing existing data"});
                }
            });

        } else {
            console.log("FINISHED EMPTY RE-INDEXING");
            callback(null,{"message":"done re-indexing empty data"})
        }


    });

};

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
                console.log('Current index is: ', meta.currentIndex);
                if (typeof meta.currentIndex !== 'undefined' && meta.currentIndex !== '' && meta.currentIndex !== 'null') {
                    let number = parseInt(meta.currentIndex.substr(meta.currentIndex.lastIndexOf("_") + 1)) + 1;
                    newIndexName = docType + "_index_" + number;
                }
                newIndexName = String(newIndexName).toLowerCase();
                console.log('New index is: ', newIndexName);


                elastic.createIndex(newIndexName, docType, newStructure, function (err, elasticResult) {

                    console.log('Elastic Search Said:', err, elasticResult);
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
                                indexData(docType, elastic, newIndexName, function (err, indexDataResponse) {
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



// noinspection JSUnresolvedVariable
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

