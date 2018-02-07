class ElasticSearch {

    execute(method='POST', path, bodyObj, callback) {

        // noinspection JSUnresolvedFunction
        let req = new AWS.HttpRequest(this.elasticSearchEndPoint);
        req.method = method;
        req.path = path;
        req.region = this.region;
        // noinspection JSValidateTypes
        req.headers['presigned-expires'] = false;
        req.headers['Host'] = this.elasticSearchEndPoint.host;
        req.headers['Content-Type'] = "application/json";
        if (bodyObj !== null) {
            req.body = JSON.stringify(bodyObj);
        }

        if (bodyObj === null || (bodyObj !== null && req.body !== false) ) {

            //noinspection JSUnresolvedVariable,JSUnresolvedFunction
            let signer = new AWS.Signers.V4(req, 'es');  // es: service code
            //noinspection JSUnresolvedFunction
            signer.addAuthorization(this.awsCredentials, new Date());

            //noinspection JSUnresolvedFunction
            let send = new AWS.NodeHttpClient();
            //noinspection JSUnresolvedFunction
            send.handleRequest(req, null, function (httpResp) {

                let respBody = '';

                // noinspection JSUnresolvedFunction
                httpResp.on('data', function (chunk) {
                    respBody += chunk;
                });
                // noinspection JSUnresolvedFunction
                httpResp.on('end', function () {
                    if (callback !== null) callback(null, respBody);

                });

            }, function (err) {
                if (callback !== null) callback(err, null);
            });

        } else {
            if (callback !== null) callback({"message":"ElasticSearch.query error: invalid json provided"}, null);
        }

    }

    //Private
    _generateMappingForStructure(structure) {

        let mapping = {
            _source: {
                enabled: true,
                includes: []
            },
            dynamic: "false",
            properties: {}
        };

        Object.keys(structure).forEach(function(field){

            let type = structure[field];
            mapping._source.includes.push(field);
            mapping.properties[field] = { type : type };

            //String and its enhanced variants
            if (['string','autocomplete','cvs','phone'].indexOf(type) !== -1 ) {
                mapping.properties[field]['fields'] = {
                    raw : {
                        type : 'string',
                        index : 'not_analyzed',
                        ignore_above : 10922
                    },
                    row_lower : {
                        type : 'string',
                        analyzer : 'keyword_lower',
                        ignore_above : 10922
                    }
                };
            }

            if (type === 'boolean') {
                mapping.properties[field]['null_value'] = false;
            }

            if (type === 'geo_point') {
                mapping.properties[field]['geohash'] = true;
                mapping.properties[field]['geohash_precision'] = 10;
            }

            if (type === 'date') {
                mapping.properties[field]['format'] = "YYYY-MM-dd HH:mm:ss.SSSSSS||YYYY-MM-dd||YYYY/MM/dd";
                mapping.properties[field]['fields'] = {
                    to_string: {
                        'type': 'string',
                        'index': 'not_analyzed'
                    }
                };
            }

            if (type === 'phone') {
                mapping.properties[field]['type'] = 'string';
            }


            if (type === 'autocomplete') {
                mapping.properties[field]['type'] = 'string';
                mapping.properties[field]['index_analyzer'] = 'autocomplete';
                mapping.properties[field]['search_analyzer'] = 'standard';
            }

            if (type === 'cvs') {
                mapping.properties[field]['type'] = 'string';
                mapping.properties[field]['index_analyzer'] = 'cvs';
                mapping.properties[field]['search_analyzer'] = 'standard';
            }


        });

        return mapping;
    }

    //private
    _generateIndexDefinition(docType, structure) {

        let indexDef = {
            settings : {
                analysis : {
                    filter : {
                        shingle : {
                            type : "shingle"
                        }
                    },
                    analyzer : {
                        keyword_lower : {
                            type : "custom",
                            tokenizer : "keyword",
                            filter : ['lowercase']
                        }
                    }
                }
            },
            mappings : {}
        };


        let mapName = docType + '_map_type';
        indexDef.mappings[mapName] = this._generateMappingForStructure(structure);

        let hasAutocomplete = false;
        let hasCsv = false;
        Object.keys(structure).forEach(function(key) {
            if (structure[key] === 'autocomplete') {
                hasAutocomplete = true;
            }
            if (structure[key] === 'cvs') {
                hasCsv = true;
            }
        });

        if (hasAutocomplete) {

            indexDef.settings['number_of_shards'] = 1;
            indexDef.settings['analysis']['filter']['autocomplete_filter'] = {
                type : 'edge_ngram',
                min_gram : 1,
                max_gram : 20
            };

            indexDef.settings['analysis']['analyzer']['autocomplete'] = {
                type : 'custom',
                tokenizer : 'standard',
                filter : [ 'lowercase', 'autocomplete_filter']
            };

        }

        if (hasCsv) {

            //reference http://stackoverflow.com/questions/29260967/elastic-search-any-way-to-make-space-separated-words-in-a-comma-separated-list
            indexDef.settings['number_of_shards'] = 1;
            indexDef.settings['analysis']['analyzer']['cvs'] = {
                type : 'pattern',
                pattern : ', ',
                lowercase : true
            };

        }

        return indexDef;


    }


    createIndex(indexName, docType, structure, callback) {
        let definition = this._generateIndexDefinition(docType,structure);
        this.execute("POST", path.join('/', indexName, docType), definition, callback);
    };


    deleteIndex(indexName, docType, callback) {
        this.execute("DELETE", path.join('/', indexName), null, callback);
    }

    deleteAlias(aliasName, indexName, callback) {
        let actions = {
            actions: [{
                remove : {
                    index: indexName,
                    alias: aliasName
                }
            }]
        };
        this.execute("POST","/_aliases", actions, callback);
    }


    putAlias(aliasName, oldIndex, newIndex, callback) {

        let actions = {
            actions: []
        };

        if (oldIndex !== '') {
            actions.actions.push({
                remove : {
                    index : oldIndex,
                    alias : aliasName
                }
            });
        }

        actions.actions.push({
            add : {
                index : newIndex,
                alias : aliasName
            }
        });

        this.execute("POST","/_aliases", actions, callback);

    }


    constructor(awsCredentials, region, elasticSearchEndPoint) {
        this.awsCredentials = awsCredentials;
        this.region = region;
        // noinspection JSUnresolvedFunction
        this.elasticSearchEndPoint = new AWS.Endpoint(elasticSearchEndPoint);
    }
}


class Dynamo {


    putMeta(type,structure,currentIndex,nextIndex,status,callback) {

        let metaTable = this.metaTable;

        let params = {
            Item: {
                "type" : { S: type },
                "structure" : { S: structure },
                "currentIndex" : { S: currentIndex },
                "nextIndex" : { S: nextIndex },
                "status" : { S: status }
            },
            TableName: metaTable,
            ReturnConsumedCapacity: "TOTAL",
            ReturnValues: "ALL_OLD"
        };


        // noinspection JSUnresolvedVariable,JSUnresolvedFunction
        this.dynamoDB.putItem(params, function(err, data) {
            if (err) {
                console.log('ERROR UPDATING ' + metaTable + ':', err);
                if (callback !== null) callback(err,null);
            }
            else {
                console.log('SUCESSFULY UPDATED ' + metaTable + ':', params.Item , data);
                if (callback !== null) callback(null,params.Item);
            }
        });

    }


    getMeta(docType, callback) {

        let metaTable = this.metaTable;
        let params = {
            TableName: metaTable,
            Key: {
                'type' : {S: docType},
            }
        };

        dynamoDB.getItem(params, function(err, data) {
            if (err) {
                callback(err,null);
            } else {
                let meta = data.Item || null;
                let item = typeof data.Item !== 'undefined' ? {
                    type : typeof meta['type'] !== 'undefined' ? meta['type']['S'] : '',
                    structure : typeof meta['structure'] !== 'undefined' ? meta['structure']['S'] : '',
                    currentIndex: typeof meta['currentIndex'] !== 'undefined' ? meta['currentIndex']['S'] : '',
                    nextIndex : typeof meta['nextIndex'] !== 'undefined' ? meta['nextIndex']['S'] : '',
                    status: typeof meta['status'] !== 'undefined' ? meta['status']['S'] : ''
                } : null;

                callback(null,item);
            }
        });
    }


    // noinspection JSMethodCanBeStatic
    indexData(docType, elastic, newIndexName, callback) {

        callback(null, {'message':'simulated from ' + this.dataTable});

    }


    constructor(dynamoDb, metaTable, dataTable) {
        // noinspection JSUnusedGlobalSymbols
        this.dynamoDB = dynamoDb;
        this.metaTable = metaTable;
        this.dataTable = dataTable;
    }

}


// noinspection JSUnresolvedFunction,NpmUsedModulesInstalled
let AWS = require('aws-sdk');
// noinspection JSUnresolvedFunction
let path = require('path');

//noinspection JSUnresolvedFunction,JSUnresolvedVariable
let region = process.env.region || 'us-west-1';
AWS.config.update({region: region});


// noinspection JSUnresolvedFunction
let awsCredentials = new AWS.EnvironmentCredentials('AWS');
// noinspection JSUnresolvedFunction
let elasticSearchEndPoint = new AWS.Endpoint(process.env.elasticURL || '');
let elastic = new ElasticSearch(awsCredentials, region, elasticSearchEndPoint);

//noinspection JSUnresolvedFunction
let dynamoDB = new AWS.DynamoDB({apiVersion: '2012-08-10'});
let dynamo = new Dynamo(dynamoDB, process.env.metaTable || 'app-Meta', process.env.dataTable || 'app-Data');


let processRecord = function(item, callback){

    let record = item['dynamodb']['NewImage'] || null;
    let oldRecord = item['dynamodb']['OldImage'] || null;

    if (record !== null) {

        let docType = String(record['type']['S']).toLowerCase();
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

                                            elastic.deleteIndex(meta.currentIndex, docType, function (err, deleteIndexResponse) {
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

