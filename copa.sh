#!/bin/sh


FILENAME="$1"  # Original zip file
CWD="/tmp/copa/"
mkdir "$CWD"
cp "$FILENAME" "$CWD"
cp "template.xlsx" "$CWD"
cp "config.ini" "$CWD"
python -m zipfile -e "$FILENAME" "$CWD"  # Extract zip file
cd "$CWD"  # cwd changes from "~/copa/" to "/tmp/copa/"
python ~/copa/ee.py "$FILENAME"  # Add excel files to the zip file copy
