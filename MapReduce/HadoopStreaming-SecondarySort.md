

```bash
$ mkdir data
$ wget https://raw.githubusercontent.com/justinjiajia/datafiles/main/flights.csv -O ~/data/flights.csv
$ du -sh data
$ head -n30 ~/data/flights.csv
```

## preprocess data

mapper_pre.py


```python
#!/usr/bin/env python3
import sys

skip = True
for line in sys.stdin:
  if skip:
    skip = False
    continue
  fields = line.strip().split(",")
  print("%s\t%s\t%s\t%s" % (fields[0]+"%02d" % int(fields[1]), fields[13], fields[9]+fields[10], fields[12]))
```

reducer_pre.py

```python
#!/usr/bin/env python3
import sys

current_month, current_dest = None, None
count = 1

for line in sys.stdin:
  month, dest, *_ = line.strip().split('\t')
  if (current_month, current_dest) == (month, dest):
    count += 1
  else:
    if current_month:
      print("%s,%s,%s" % (current_month, current_dest, count))
      count = 1
    current_month = month
    current_dest = dest

if (current_month, current_dest) == (month, dest):
  print("%s,%s,%s" % (current_month, current_dest, count))
```


```bash
$ chmod +x mapper_pre.py reducer_pre.py
$ cat  data/flights.csv | ~/mapper_pre.py | sort -k1,2 | ~/reducer_pre.py > inbounds.txt
```


## secondary sort

mapper.py

```python
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    month, dest, count = line.strip().split(",")
    print("%s,%s,%s" % (month, count, dest))
```


reducer.py

```python
#!/usr/bin/env python3
import sys

current_month = None

for line in sys.stdin:
    month, count, dest = line.strip().split(',')
    if current_month == month: next
    else:
        print("%s,%s,%s" % (month, count, dest))
        current_month = month
```


```bash
$ chmod +x mapper.py reducer.py
$ cat inbounds.txt | ~/mapper.py | sort -t, -k1,1 -k2,2nr | ~/reducer.py
```

```bash
$ mapred streaming -D mapreduce.job.reduces=2 \
> -D stream.map.output.field.separator=, \
> -D stream.num.map.output.key.fields=2 \
> -D stream.reduce.input.field.separator=, \
> -D map.output.key.field.separator=, \
> -D mapred.text.key.partitioner.options=-k1,1 \
> -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
> -D mapreduce.partition.keycomparator.options='-k1,1 -k2,2nr' \     # double-quoted string "-k1,1 -k2,2nr" also works; mapred.output.key.comparator.class does not work here
> -files mapper.py,reducer.py \
> -input input_dir_on_HDFS \
> -output output_dir_on_HDFS \
> -mapper mapper.py -reducer reducer.py \
> -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner
```


```bash
$ hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
> -D mapreduce.job.reduces=2 \
> -D stream.map.output.field.separator=, \
> -D stream.num.map.output.key.fields=2 \
> -D stream.reduce.input.field.separator=, \
> -D map.output.key.field.separator=, \
> -D mapreduce.partition.keypartitioner.options=-k1,1 \
> -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator  \
> -D mapreduce.partition.keycomparator.options='-k1,1 -k2,2nr' \     # double-quoted string "-k1,1 -k2,2nr" also works
> -files ~/mapper.py,~/reducer.py \
> -input input_dir_on_HDFS \
> -output output_dir_on_HDFS \
> -mapper mapper.py -reducer reducer.py \
> -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
```


if you don't to use `-D stream.reduce.input.field.separator=,`
then you need to modify the reducer script to:


```python
#!/usr/bin/env python3
import sys

current_month = None

for line in sys.stdin:
    key, value = line.strip().split('\t')
    month, count = key.split(',')
    if current_month == month: next
    else:
        print("%s,%s,%s" % (month, count, value))
        current_month = month
```
