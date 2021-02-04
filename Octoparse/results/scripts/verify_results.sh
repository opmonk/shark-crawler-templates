#!/bin/sh

# Please modify variables according to environment
#profile=prod
#bucketName=ipshark-crawlers-prod
#latestDate=2021-02-02
profile=staging
bucketName=ipshark-crawlers-qa
latestDate=2021-02-04

downloadDir=./s3_output
folderKey=preprocess/octoparse-aliexpress

# Obtain latest list of files that were processed with the given date
fileList=`aws s3api list-objects-v2 --bucket $bucketName --query 'Contents[?contains(LastModified, \`'${latestDate}'\`)].Key' --profile $profile --output=text --prefix $folderKey`

# Prep the download directory.  This is only needed if the aws cp command is uncommented below.
if [ ! -d $downloadDir ]
then
	mkdir $downloadDir
fi

# Traverse list of files to determine if all files expected are found
for file in $fileList
do	
	echo $file

	# PLEASE NOTE: Uncomment the following line if you'd like to download to local directory for verification.
	# But beware that this is creating a lot of calls across the wire.  So only perform this when you really
	# need to check all files.
	# aws s3 cp "s3://$bucketName/$file" $downloadDir --profile $profile
done

