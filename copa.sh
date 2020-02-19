#!/bin/sh


FILENAME="$1"  # Original zip file
CWD="/tmp/copa/"
mkdir -p "$CWD"
cp "$FILENAME" "$CWD"
cp "template.xlsx" "$CWD"
cp "config.ini" "$CWD"
cd "$CWD"  # cwd changes from "~/copa/" to "/tmp/copa/"
python ~/copa/ee.py "$FILENAME"  # Add excel files to the zip file copy
