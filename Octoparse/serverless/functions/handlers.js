// const { AWS } = require('../config');

'use strict';

const aws = require('aws-sdk');

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
        promises.push(_processZipFile(record.s3.object.key));
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

function _processZipFile(key) {
  console.log('Received S3 Key:', key);
	// const params = {
  //   Bucket: "octoparse-qa"
	// 	Key: key,
	// };
  //
	// return await s3.getObject(params).promise()
	// 	 .then(async (results) => {
  //      return new Promise((resolve, reject) => {
  //          if (err) {
  //              console.error('Error Creating Printing Key', err);
  //              reject(err);
  //          }
  //          resolve();
  //      });
  //    });
}
