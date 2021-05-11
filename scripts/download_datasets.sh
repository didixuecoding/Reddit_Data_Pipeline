#!/usr/bin/env bash

set -e

USAGE="$0 <DATE>"

if [ "$#" -ne 1 ]; then
    echo $USAGE
    echo "ERROR: Wrong number of arguments."
    exit 1
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT_DIR="$SCRIPT_DIR/.."
DATA_FOLDER="$ROOT_DIR/input"

YEAR=`echo $1 | cut -d'-' -f1`
MONTH=`echo $1 | cut -d'-' -f2`
DAY=`echo $1 | cut -d'-' -f3`

echo "Download datasets..."

mkdir -p $DATA_FOLDER

function download_dataset_authors() {
	URL=$1
	FILE=`basename $URL`
    FILEPATH="$DATA_FOLDER/Authors/$FILE"
    if [ ! -e "$FILEPATH" ]; then
        echo "Download file $FILE from pushshift.io"
        curl -L -o "$FILEPATH" $URL  
    else
        echo "File $FILE found. Skip download."
    fi
}

function download_dataset_subreddits() {
	URL=$1
	FILE=`basename $URL`
    FILEPATH="$DATA_FOLDER/Subreddits/$FILE"
    if [ ! -e "$FILEPATH" ]; then
        echo "Download file $FILE from pushshift.io"
        curl -L -o "$FILEPATH" $URL
    else
        echo "File $FILE found. Skip download."
    fi
}

function download_dataset_submissions() {
	URL=$1
	FILE=`basename $URL`
    FILEPATH="$DATA_FOLDER/Submissions/$FILE"
    if [ ! -e "$FILEPATH" ]; then
        echo "Download file $FILE from pushshift.io"
        curl -L -o "$FILEPATH" $URL
    else
        echo "File $FILE found. Skip download."
    fi
}

# Download Authors dataset
URL="http://files.pushshift.io/reddit/authors/RA_78M.csv.zst"
download_dataset_authors $URL

# Download Subreddits dataset
URL="http://files.pushshift.io/reddit/subreddits/reddit_subreddits.ndjson.zst"
download_dataset_subreddits $URL

# Download Submission dataset (2018-01-01 to 2018-03-31)
URL="http://files.pushshift.io/reddit/submissions/daily/RS_$YEAR-$MONTH-$DAY.xz"
download_dataset_submissions $URL
