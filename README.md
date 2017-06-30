# boltq

boltdb command line query interface

```sh
$ # list buckets
$ boltq some.db
foo
bar
$ # list keys
$ boltq some.db foo
k1
k2
$ # get value
$ boltq some.db foo k1
value1
$ # set a new value
$ boltq some.db foo k1 value9000
$ boltq -v some.db foo k1
value9000
$ # The -v is verbose mode which when reading values simply appends a newline
$
$ # Sub-buckets can be listed with -v:
$ boltq -v some.db bar
b1 (bucket)
b2 (bucket)
$ boltq some.db bar.b1
eggs
spam
```
