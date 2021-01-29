Octoparse Automation
To install serverless functions on your local environment:

#####################
# Setup: Obtain Source Code
#####################
$ git clone https://github.com/opmonk/shark-crawler-templates.git .
$ cd shark-crawler-templates/Octoparse/serverless

#####################
# Setup: AWS configuration: Serverless .env
#####################
# Change AWS_USERNAME to your specific username.  Please note, this variable controls
# the resources that are created to allow for testing in 
# separate S3 buckets in the QA environment.  You will need to cleanup properly to
# ensure costs are minimized.
$ vi ops/environments/.env

# Then run following command to make sure the AWS_USERNAME is interpolated
# in the .env file at the root level.
$ ./setup.sh ops/environments/.env
$ more .env

#####################
# Deploy Local: Serverless
#####################
$ SLS_DEBUG=* serverless deploy --aws-profile qa --stage dev --region us-east-1
# Need to make sure there's a preprocess folder in ${env.AWS_OCTOPARSE_CRAWLERS_BUCKET_NAME}

#####################
# Deploy Staging: Serverless
####################
$ cp ops/environments/.env.staging .env
$ cp resources/serverless-resources.staging.yml 
$ SLS_DEBUG=* serverless deploy --aws-profile staging --region us-east-2

#####################
# Test: To Upload Raw Results: Either perform the following on commandline OR Upload through the AWS console.
# Be sure to upload results to ${env.AWS_OCTOPARSE_RAW_BUCKET_NAME} specified in your .env file
#####################
$ cd ../results/raw/01262021/
$ for x in `ls .` ; do
     mv -- "$x" "${x//[()]/_}"
  done
$ export AWS_OCTOPARSE_RAW_BUCKET_NAME=octoparse-qa-beckychu
$ aws s3 cp AliExpress-Production-CB-11042020_1_.zip s3://${AWS_OCTOPARSE_RAW_BUCKET_NAME}/Aliexpress/01262021/AliExpress-Production-CB-11042020_1_.zip
$ aws s3 cp DHGate-Production-CB-11042020_1_.zip s3://${AWS_OCTOPARSE_RAW_BUCKET_NAME}/DHGate/01262021/DHGate-Production-CB-11042020_1_.zip
$ aws s3 cp Bukalapak-Production-CB-11112020.zip s3://${AWS_OCTOPARSE_RAW_BUCKET_NAME}/DHGate/01262021/DHGate-Production-CB-11042020_1_.zip

#####################
# Test: To Verify Serverless Function results
#####################
$ cd <your local results directory>
$ mkdir -p preprocess/Aliexpress/01262021/
$ cd preprocess/Aliexpress/01262021/
$ aws s3 sync s3://${env.AWS_OCTOPARSE_CRAWLERS_BUCKET_NAME}/preprocess/octoparse-aliexpress/ .
$ wc -l *		# total preprocess count
$ ls | wc -l		# total preprocess header count

$ cd <your local results directory>
$ mkdir -p raw/Aliexpress/01262021
$ cd raw/Aliexpress/01262021
$ aws s3 sync s3://${env.AWS_OCTOPARSE_RAW_BUCKET_NAME}/Aliexpress/01262021/ .
# The following calculation should work out:
#  total raw count = total preprocess count - total preprocess header count
$ wc -l *		# total raw count

# Please note: For DHGate, you'll also need to take into account emtpy search_keys
#  total raw count - empty_searchkeys = total preprocess count - total preprocess header count
$ grep ',,$' * | wc -l  # empty searchkeys

#####################
# Full Cleanup
#####################
# On AWS Console, delete CloudWatch events
# On AWS Console, delete DynamoDB Items
# On AWS Console, delete s3 ${AWS_OCTOPARSE_RAW_BUCKET_NAME} DHGate/Aliexpress/Bukalapak keys
# On AWS Console, delete s3 ${AWS_OCTOPARSE_CRAWLERS_BUCKET_NAME} octoparse-dhgate/octoparse-aliexpress/octoparse-bukalapak keys

#####################
# Troubleshooting: Make sure your local environment does NOT override .env variables
#####################
$ env | grep AWS
AWS_CLIENT_TIMEOUT=600000
AWS_OCTOPARSE_DYNAMO_TABLE=octoparse-crawls
AWS_OCTOPARSE_CRAWLERS_BUCKET_NAME=ipshark-crawlers-octoparse-qa
AWS_OCTOPARSE_RAW_BUCKET_NAME=octoparse-qa
AWS_FUNCTION_ROLE_ARN=arn:aws:iam::929207119037:role/ipshark-qa-lambda-role

$ unset AWS_CLIENT_TIMEOUT AWS_OCTOPARSE_DYNAMO_TABLE AWS_OCTOPARSE_CRAWLERS_BUCKET_NAME AWS_OCTOPARSE_RAW_BUCKET_NAME AWS_FUNCTION_ROLE_ARN
