#!/bin/bash

URL=https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz
FOLDER="/input"
SAVEPATH=$(pwd -P)$FOLDER
FILE_NAME=soc-LiveJournal1.txt

echo "$SAVEPATH"

echo "-----Downloading data from "$URL" -----"
#wget --directory-prefix "$SAVEPATH" "$URL"
#curl --url "$URL" --output "$SAVEPATH/"
curl --url $URL | gunzip -c >> "$SAVEPATH"/"$FILE_NAME"
echo "-----Download complete-----"

echo "-----Deleting file header rows----"
sed '1,4d' "$SAVEPATH"/"$FILE_NAME" > "$SAVEPATH"/tmpfile
mv "$SAVEPATH"/tmpfile "$SAVEPATH"/"$FILE_NAME"
rm "$SAVEPATH"/tmpfile
echo "-----Done----"
