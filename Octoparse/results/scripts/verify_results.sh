#!/bin/sh

bucketName=ipshark-crawlers-qa
latestDate=2021-02-04
downloadDir=./s3_output
mkdir $downloadDir

# obtain latest list of files that were processed with the given date
fileList=`aws s3api list-objects-v2 --bucket $bucketName --query 'Contents[?contains(LastModified, \`2021-02-04\`)].Key' --profile staging --output=text | grep inbox`

for file in $fileList
do
	aws s3 cp "s3://$bucketName/$file" $downloadDir --profile staging
done

