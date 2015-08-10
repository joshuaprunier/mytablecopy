# mytablecopy
A command line tool for copying MySQL tables between databases. Performance is very close to a copy done with mysqldump piped to the mysql command. The problem with that approach this tool aims to solve is renaming without sed and retrying of failed inserts. The primary features are:
  * A table can be copied to a different schema destination.
  * A table can be renamed.
  * The -where clause flag can be used to copy a subset of rows.
  * The -ignore flag does not drop the destination table if it exists and uses insert ignore to append rows.
  

Installation
--
```shell
$ go get -d github.com/joshuaprunier/mytablecopy

$ ./build.sh

Building mytablecopy version
1.0.19-c98a96b-20150810.130749


Building Linux
  mytablecopy - OK

Building Windows
  mytablecopy.exe - OK

Building Darwin
  mytablecopy - OK

Done!

$ls -R bin
bin:
darwin  linux  windows

bin/darwin:
mytablecopy

bin/linux:
mytablecopy

bin/windows:
mytablecopy.exe
```

Usage
--
```shell
mytablecopy version 1.0.19-c98a96b-20150810.130749

USAGE:
mytablecopy -srcuser=jprunier -srcpass= -srchost=db1 -srctable=test.mytable -tgthost=db2
mytablecopy -srcuser=jprunier -srcpass=mypass -srchost=db1 -srctable=test.mytable -where="1=1 limit 1000" -tgtuser=root -tgtpass=pass123 -tgthost=db2 -tgttable=scratchpad.newtable

SOURCE DATABASE FLAGS
=====================
-srcuser: Source Username (required)
-srcpass: Source Password (interactive prompt if blank)
-srchost: Source Database (localhost assumed if blank)
-srcport: Source MySQL Port (3306 default)
-srcsocket: Source MySQL Socket File
-srctable: Fully Qualified Source Tablename: ex. schema.tablename (required)
-where: Where clause to apply to source table select

TARGET DATABASE FLAGS
=====================
-tgtuser: Target Username (source username used if blank)
-tgtpass: Target Password (source password used if blank)
-tgthost: Target Database (required)
-tgtport: Target MySQL Port (3306 default)
-tgtsocket: Target MySQL Socket File
-tgttable: Fully Qualified Target Tablename: ex. schema.tablename (source tablename used if blank)
-ignore: Do insert ignore's and enable the -append flag (false default)
-append: Don't drop the destination table before copying (false default)

DEBUG FLAGS
===========
-debug_cpu: CPU debugging filename
-debug_mem: Memory debugging filename
-version: Version information
-v: Print more information (false default)
```

Examples
--
##### Basic usage - test.mytable is copied from db1 to db2
```shell
mytablecopy -srcuser=jprunier -srcpass=mypass -srchost=db1 -srctable=test.mytable \
-tgtuser=root -tgtpass=pass123 -tgthost=db2
```

##### Same account on source & target - test.mytable is copied from db1 to db2
```shell
mytablecopy -srcuser=jprunier -srcpass=mypass -srchost=db1 -srctable=test.mytable -tgthost=db2
```

##### Prompt for password - test.mytable is copied from db1 to db2
```shell
mytablecopy -srcuser=jprunier -srcpass= -srchost=db1 -srctable=test.mytable -tgthost=db2
```

##### Schema and table rename - test.mytable is copied from db1 to scratchpad.newtable on db2
```shell
mytablecopy -srcuser=jprunier -srcpass=mypass -srchost=db1 -srctable=test.mytable \
-tgtuser=root -tgtpass=pass123 -tgthost=db2 -tgttable=scratchpad.newtable
```

##### Where clause - Only 500,000 rows are copied from test.mytable on db1 to db2 
```shell
mytablecopy -srcuser=jprunier -srcpass=mypass -srchost=db1 -srctable=test.mytable -where="1=1 limit 500000" \
-tgtuser=root -tgtpass=pass123 -tgthost=db2
```

##### Combine multiple sources to a single destination - All rows from mytable1 & mytable2 on db1 and mytable3 on db3 are copied to test.new on db2 
```shell
mytablecopy -srcuser=jprunier -srcpass= -srchost=db1 -srctable=test.mytable1 -tgthost=db2 \
-tgttable=test.new -ignore
mytablecopy -srcuser=jprunier -srcpass= -srchost=db1 -srctable=test.mytable2 -tgthost=db2 \
-tgttable=test.new -ignore
mytablecopy -srcuser=jprunier -srcpass= -srchost=db3 -srctable=test.mytable3 -tgthost=db2 \
-tgttable=test.new -ignore

```
##### Verbose - Show information during copy
```shell
mytablecopy -srcuser=jprunier -srcpass=mypass -srchost=db1 -srctable=test.mytable -where="1=1 limit 100000" \
-tgtuser=root -tgtpass=pass123 -tgthost=db2
A '.' will be shown for every 10,000 CSV rows written
..........

100000 rows written
Total runtime = 12.994247028s
```

License
--
[MIT] (LICENSE)
