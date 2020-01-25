#!/bin/sh


FILENAME="$1"
INPUTDIR="/tmp/copa/"
mkdir "$INPUTDIR"
cp "$FILENAME" "/tmp/copabackup/"

python -m zipfile -e "$FILENAME" "$INPUTDIR"
python -m design "$INPUTDIR" "$FILENAME"
