There is a boring stuff about transforming informations from pdf to excel.
Instead of doing manully, this repo gives a tool.
Using an excel file as template, it searches the keywords in pdf files,
then puts the result into a new excel file.
There are some rules for the template:
First row is about headers. First column is an index.
Row 'value' is aoubt default value of each header.
Row 'dropdownlist' is for data validation.
Row 'formula' can be used to write formula into excel.
Row 'width' and 'format' are about styling.
Row 're' 'derive' are used by main python module to get right values. In
order to make them work, we must have a config file, which contanin the
parameters of the names.

Usage:
Extract a zip file into current working directory. We need template.xlsx
and config.ini two files in addition.
$ python -m ee "name of zip file"

There is nothing about code needed except regular expression, so anybody
who familiars with regular expression can use this repo.
The more computer can do, the less work left for human.
Make sure tika server is tika-server-1.19, text is parsed by tika.

This is my first script, I am glad to recieve your help and question.
We might communicate by email although my response maybe slow.
My emmail: wushuowow@gmail.com
