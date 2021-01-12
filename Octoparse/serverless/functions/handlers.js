// const { AWS } = require('../config');

'use strict';

const aws = require('aws-sdk');
//const zlib = require("zlib");
const s3Unzip = require("s3-unzip");
const s3 = new aws.S3({
	apiVersion: '2006-03-01'
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
exports.s3OctoparseRawEventListener = async (event, context, callback) => {
    console.log('Received S3 Event:', event);

    const promises = [];
    event.Records.forEach(record => {
        promises.push(_processZipFile(record.s3, record.s3.object.key));
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

function _processZipFile(s3Client, key) {
  console.log('Received S3 Key:', key);

  // Need to send file to python script for parsing.  If a zipfile,
  // python script needs to first unzip and take as input


	// const params = {
  //   Bucket: "octoparse-qa",
	// 	Key: key,
	// };
  return new s3Unzip({
      bucket: "octoparse-qa",
      file: key,
      deleteOnSuccess: false,
      verbose: false
    }, function(err, success){
      if (err) console.error(err, key);
      else console.log(success, key);
  });
  // return await s3.getObject(params, (err, data) => {
  //   if (err) {
  //       console.error(err);
  //       const message = `Error getting object ${key} from bucket ${bucket}. Make sure they exist and your bucket is in the same region as this function.`;
  //       console.error(message);
  //       callback(message);
  //   } else {
  //       zlib.unzip(data.Body, function (err, result) {
  //           if (err) {
  //               console.error('UNZIP PROCESSING FAILED', err);
  //           } else {
  //               var extractedData = result;
  //               s3.putObject({
  //               Bucket: "bucketName",
  //               Key: "filename",
  //               Body: extractedData,
  //               ContentType: 'content-type'
  //               }, function (err) {
  //                    console.log('uploaded file: ' + err);
  //               });
  //           }
  //       });
  //   }
  // });

}
