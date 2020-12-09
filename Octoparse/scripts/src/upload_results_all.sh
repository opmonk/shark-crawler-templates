#!/bin/bash

if [[ $1 == "" ]]
then
   echo "./upload_results_all.sh <output directory>"
   exit 2
fi

files=`ls $1`
for file in $files
do
   echo $file | awk -F'_' ' { print "curl -XPOST '\''https://tt56usy83j.execute-api.us-east-1.amazonaws.com/dev/crawls'\'' -d'\'''\{'\"crawlerId\": " $2 ", \"assetId\": " $3 ", \"keywordId\": "$4 ", \"runToken\": " $1 "}'\''" } '
   #echo $file | awk -F'_' ' { print "crawlerId:" $2 " assetId: " $3 " keywordId: "$4 " runToken:" $1 } '
done
