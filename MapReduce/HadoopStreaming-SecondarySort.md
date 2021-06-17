

```bash
wget -O flights.csv https://raw.githubusercontent.com/justinjiajia/datafiles/main/flights.csv
```

## preprocess data

mapper_pre.py


```python3

#!/usr/bin/env python3
import sys

n=1
for line in sys.stdin:
    if n!=1:
       fields = line.strip().split(",")
       print("%s\t%s\t%s\t%s" % (fields[0]+("0"+fields[1] if len(fields[1])<2 else fields[1]), fields[13], fields[9]+fields[10], fields[12]))
    n+=1
```

reducer_pre.py

```python3

#!/usr/bin/env python3
import sys

current_month, current_dest = None, None
count = 0

for line in sys.stdin:
    line = line.strip()
    month, dest, *_ = line.split('\t')
    if (current_month, current_dest) == (month, dest):
        count += 1
    else:
        if current_month:
            print("%s,%s,%s" % (current_month, current_dest, count))
            count=0
        current_month = month
        current_dest = dest

if (current_month, current_dest) == (month, dest): print("%s,%s,%s" % (current_month, current_dest, count))

```


```bash
$ chmod +x mapper_pre.py reducer_pre.py
$ cat  flights.csv | ~/mapper_pre.py | sort -k1,2 | ~/reducer_pre.py > inbounds.txt
```


## secondary sort

mapper.py

```python3

#!/usr/bin/env python3
import sys


for line in sys.stdin:
    fields = line.strip().split(",")
    print("%s,%s,%s" % (fields[0], fields[2], fields[1]))
```


reducer.py

```python3
#!/usr/bin/env python3
import sys

current_month = None

for line in sys.stdin:
    line = line.strip()
    month, count, dest = line.split(',')
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
$ hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
> -D mapreduce.job.reduces=2 \
> -D mapreduce.map.output.key.field.separator=, \
> -D stream.num.map.output.key.fields=2 \
> -D mapreduce.partition.keypartitioner.options=-k1,1 \
> -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
> -D mapreduce.partition.keycomparator.options='-k1,1 -k2,2nr' \      # "-k1,1 -k2,2nr" still works
> -input input_dir_on_HDFS \
> -output output_dir_on_HDFS \
> -mapper mapper.py \
> -reducer reducer.py \
> -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
> -file ~/mapper.py \
> -file ~/reducer.py

```
