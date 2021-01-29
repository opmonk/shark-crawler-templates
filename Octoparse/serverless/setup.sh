#! /bin/bash
  
if [ $# -eq 0 ]; then
        echo "Error: Requires file as argument"
	exit 1
fi

INPUT_FILE=$1
OUTPUT_FILE=".env"

echo "Copying $1 file w/ interpolation to ./.env"

printf "" > $OUTPUT_FILE
LINES=$(grep -v '^#' $INPUT_FILE)
for i in $LINES; do
        eval "export $i";
        KEY=$(echo $i | cut -f1 -d"=")
        echo "$KEY=${!KEY}" >> $OUTPUT_FILE
done

