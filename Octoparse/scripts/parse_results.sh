#!/bin/bash

results_file=../r_results_run_demo/DHGate-Prod_Large_ImageUrls_Demo.csv
tmp_results_file=${results_file}.tmp
p_results_dir=../p_results_run_demo
mkdir $p_results_dir
run_token_date=`date +%s`

# Need to remove <U+EEFF>
awk '{ gsub(/\xef\xbb\xbf/,""); print }' $results_file > $tmp_results_file
mv $tmp_results_file $results_file

# extracts the list of keywords to iterate through and create filenames for
# Count how many lines do not contain the seachkey?
cat $results_file | sed -e 's/"[^"]*"//g' | awk -F',' ' { print $8 $9 }' | uniq | grep -v searchkey > keywords.txt


################
# Process results
################
for keyword in `cat keywords.txt`
do 
    ################
    # Setup filename as <timestamp>-DHGate-<keyword> and parse results to individual files based on keyword(s)
    ################
    keyword_trimmed=`echo $keyword | sed 's/&catalog.*//'`
    run_token=$run_token_date-DHGate-$keyword_trimmed
    p_results_file=$run_token.csv

    # Create header for processed results file
    header_line=`head -n 1 $results_file`
    echo $header_line > $p_results_dir/$p_results_file
done

for keyword in `cat keywords.txt`
do
    ################
    # Should be setup as a function name
    ################
    keyword_trimmed=`echo $keyword | sed 's/&catalog.*//'`
    run_token=$run_token_date-DHGate-$keyword_trimmed
    p_results_file=$run_token.csv

    # --------------------
    # Is this accurate ?  e.g. grep Seagate (we need to instead grep on field 8 or 9.
    #grep $keyword $results_file >> $p_results_dir/$p_results_file

    #grep ",$keyword[&^,]*$" $results_file >> $p_results_dir/$p_results_file
    #grep ",$keyword[&^,]*,$" $results_file >> $p_results_dir/$p_results_file
    grep ",$keyword,$" $results_file >> $p_results_dir/$p_results_file
    grep ",$keyword$" $results_file >> $p_results_dir/$p_results_file
    #echo | awk -v awkkeyword="$keyword" -F',' '$8~awkkeyword' $results_file >> $p_results_dir/$p_results_file
    #echo | awk -v awkkeyword="$keyword" -F',' '$9~awkkeyword' $results_file >> $p_results_dir/$p_results_file
    # --------------------

    # echo $p_results_dir/$p_results_file
done

for keyword in `cat keywords.txt`
do

    ################
    # Should be setup as a function name
    ################
    keyword_trimmed=`echo $keyword | sed 's/&catalog.*//'`
    run_token=$run_token_date-DHGate-$keyword_trimmed

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
    #curl -XPOST 'https://tt56usy83j.execute-api.us-east-1.amazonaws.com/dev/crawls' -d'{"crawlerId": 8, "assetId": $assetId, "keywordId": $keywordId, "runToken": "$run_token"}'
done


