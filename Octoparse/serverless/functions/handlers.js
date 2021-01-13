'use strict';

const aws = require('aws-sdk');
const Utils = require("../helpers/util");
const s3 = new aws.S3({
	apiVersion: '2006-03-01'
});

// Example of JS Lambda calling Python Lambda
// https://lorenstewart.me/2017/10/02/serverless-framework-lambdas-invoking-lambdas/
const lambda = new aws.Lambda({
  region: "us-east-1"
});
/**
 *
 * AWS S3 event handler
 * This lambda is triggered when a new file is uploaded
 *
 * @param {object} event object
 * @param {object} context
 * @param {func} callback
 *
 * @returns {object}
 */
exports.s3OctoparseRawCsvEventListener = async (event, context, callback) => {
    console.log('Received S3 CSV Event:', event);

    const promises = [];
    event.Records.forEach(record => {
        promises.push(_processCsvFile(record.s3.object.key));
    });

    return Promise.all(promises)
        .then(() => {
            console.log('EVENT PROCESSING SUCCESS');
            return callback(null, { result: 'OK' });
        })
        .catch(err => {
            console.error('EVENT PROCESSING FAILED', err);
            return callback(err);
        });
};

function _processCsvFile(key) {
  console.log('Received S3 Csv Key:', key);
   const params = {
     FunctionName: "octoparse-postprocess-service-dev-OctoparsePostProcess"
   };

   return lambda.invoke(params, function(error, data) {
     if (error) {
       console.error(JSON.stringify(error));
       return new Error(`Error printing messages: ${JSON.stringify(error)}`);
     } else if (data) {
       console.log("OctoparsePostProcess", data);
     }
   });
}
/**
 *
 * AWS S3 event handler
 * This lambda is triggered when a new file is uploaded
 *
 * @param {object} event object
 * @param {object} context
 * @param {func} callback
 *
 * @returns {object}
 */
exports.s3OctoparseRawZipEventListener = async (event, context, callback) => {
    console.log('Received S3 Zip Event:', event);

    const promises = [];
    event.Records.forEach(record => {
        promises.push(_processZipFile(record.s3.object.key, callback));
    });

    return Promise.all(promises)
        .then(() => {
            console.log('EVENT PROCESSING SUCCESS');
            return callback(null, { result: 'OK' });
        })
        .catch(err => {
            console.error('EVENT PROCESSING FAILED', err);
            return callback(err);
        });
};

function _processZipFile(key, callback) {
  console.log('Received S3 Zip Key:', key);

  if (callback === undefined) {callback = function(err, success) {};}
  Utils.decompress({
    bucket: "octoparse-qa",
    file: key,
    deleteOnSuccess: true,
    verbose: true
  }, callback);
}
