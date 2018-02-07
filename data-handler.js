'use strict';


//noinspection JSUnresolvedVariable
module.exports.dataHandler = (event, context, callback) => {

  const response = {
    statusCode: 200,
    body: JSON.stringify({
      message: 'Canvas DB Handler Log',
      input: event,
      elasticUrl : process.env.elasticURL || ''
    })
  };


  callback(null, response);
  console.log(response);

};