'use strict';

const aws = require('aws-sdk');
// aws.config.update({
//     maxRetries: 0,
//     httpOptions: {
//         timeout: 450000,
//         connectTimeout: 450000
//     }
// });
const Utils = require("../helpers/util");
const s3 = new aws.S3({
	apiVersion: '2006-03-01'
});
const lambda = new aws.Lambda();

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
    for (let i = 0; i < event.Records.length; i++) {
    		const key = event.Records[i].s3.object.key;
    		promises.push(_processCsvFile(key));
        console.log('Done processing Key:', key);
    }
    return await Promise.allSettled(promises);
};

async function _processCsvFile(key) {
  console.log('Received S3 Csv Key:', key);

  // Lambda invokation to python parser occurs in this function:
  // Need to determine the following params to pass into octoparse-post-process
  // -i input file
  // -o output directory
  // -p platform
  // These values can be obtained by the s3 object's folder structure. i.e.:
  //      DHGate/1122021/DHGate-Production-CB-11042020-adidas.csv
  // should generate the following parameter values to pass into lambda invoke:
  //      -p dhgate
  //      -o preprocess/octoparse-dhgate/
  //      -i DHGate/1122021/DHGate-Production-CB-11042020-adidas.csv
  var filenamePartsArray = key.split("/");
  // Expect that the folder created will always contain the platform name first
  var platform = filenamePartsArray[0].toLowerCase();
  var output_dir = "preprocess/octoparse-" + platform + "/";

  const params = {
     FunctionName: "octoparse-postprocess-dev-octoparse-post-process",
     Payload: '{"input_file":"' + key + '", "output_dir":"' + output_dir + '", "platform":"' + platform + '"}'
  };
  console.log("octoparse-post-process params", params);
  var data = '';

  try {
     // Need to make this call idempotent through use of DynamoDB
     data = await lambda.invoke(params, function(error, data) {
        console.log("Lambda Invoke:", params);
        if (error) {
           console.error(JSON.stringify(error));
           return new Error(`Error printing messages: ${JSON.stringify(error)}`);
        } else if (data) {
           console.log("OctoparsePostProcess", data);
        }
     }).promise();
  } catch (err) {
     console.log(err);
     return err;
  }
  return data;
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
    bucket: process.env.AWS_OCTOPARSE_RAW_BUCKET_NAME,
    file: key,
    deleteOnSuccess: true,
    verbose: true
  }, callback);
}
