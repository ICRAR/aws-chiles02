```bash
find . -name '*_deepfield.ms' -type d | xargs du -csh
```

## find
* -type d - only a directory

## du
* -c gives a grand total
* -s summarise the direcory so it is the total of all the subdirectories
* -h human readable