#!/bin/bash

p_results_dir=../p_results_run2/small

files=`ls $p_results_dir`

for file in $files
do
    run_token=`echo $file | awk -F'.' ' { print $1 } '`
    keyword=`echo $run_token | awk -F'-' ' { print $3 } '`

    ################
    # Upload results to S3 bucket
    ################
    # Need to join between full_results.csv & $p_results_file
    crawlerId=8

    # find assetId & keywordId
    line=`grep $keyword ../sql/full_results.csv`
    assetId=`echo $line | awk -F',' ' { print $3 } '`
    keywordId=`echo $line | awk -F',' ' { print $5 } '`

    # echo "$run_token, $assetId, $keywordId"
    echo "curl -XPOST 'https://tt56usy83j.execute-api.us-east-1.amazonaws.com/dev/crawls' -d'{\"crawlerId\": $crawlerId, \"assetId\": $assetId, \"keywordId\": $keywordId, \"runToken\": \"$run_token\"}'"
    curl -XPOST 'https://tt56usy83j.execute-api.us-east-1.amazonaws.com/dev/crawls' -d'{"crawlerId": 8, "assetId": $assetId, "keywordId": $keywordId, "runToken": "$run_token"}'
done


