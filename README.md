# Hadoop Broadcast Join

## Task

User Hadoop Broadcast Join to join two datasets available [here](https://dumps.wikimedia.org/other/clickstream/)

For the first month get 1000 most popular pages.

For the second one make join with 1000 most popular pages of the first month.

## Solution

### Preparation

`$ wget https://dumps.wikimedia.org/other/clickstream/2019-03/clickstream-enwiki-2019-03.tsv.gz`

`$ wget https://dumps.wikimedia.org/other/clickstream/2019-04/clickstream-enwiki-2019-04.tsv.gz`

`$ gzip -d *.gz`

`$ hadoop fs -mkdir -p /user/tg/input`

`$ hadoop fs -put *.tsv /user/tg/input`

`$ export JAVA_HOME=/usr/java/default`

**USE JAVA 8 ONLY!**

`export PATH=${JAVA_HOME}/bin:${PATH}`

`export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar`

### Step 1

Compile .jar for mapreduce data from firs month:

`$ javac -cp ~/hadoop-3.1.2/share/hadoop/client/*:~/hadoop-3.1.2/share/hadoop/common/* BroadcastJoinTop1000MapRed.java`

`$ jar cf BroadcastJoinTop1000MapRed.jar *.class`

`$ rm *.class`

Run first job

`$ hadoop-3.1.2/bin/hadoop jar BroadcastJoinTop1000MapRed.jar BroadcastJoinTop1000MapRed -Dmapreduce.map.memory.mb=1000 -Dmapreduce.map.java.opts.max.heap=800 -Dmapreduce.reduce.memory.mb=1000 -Dmapreduce.reduce.java.opts.max.heap=800 -Dmapreduce.job.reduces=4 /user/tg/input/clickstream-enwiki-2019-03.tsv /user/tg/output/`

### Step 2

Compile .jar for sorting

`$ javac -cp ~/hadoop-3.1.2/share/hadoop/client/*:~/hadoop-3.1.2/share/hadoop/common/* BroadcastJoinTop1000Sort.java`

`$ jar cf BroadcastJoinTop1000Sort.jar *.class`

`$ rm *.class`

Run sorting job

`$ hadoop-3.1.2/bin/hadoop jar BroadcastJoinTop1000Sort.jar BroadcastJoinTop1000Sort -Dmapreduce.map.memory.mb=1000 -Dmapreduce.map.java.opts.max.heap=800 -Dmapreduce.reduce.memory.mb=1000 -Dmapreduce.reduce.java.opts.max.heap=800 -Dmapreduce.job.reduces=4 /user/tg/output/ /user/tg/sorted`

`$ hadoop-3.1.2/bin/hadoop fs -text /user/tg/sorted/* | head -n1000 > top1000.txt`

### Step 3

Compile .jar for broadcast joining

`$ javac -cp ~/hadoop-3.1.2/share/hadoop/client/*:~/hadoop-3.1.2/share/hadoop/common/* BroadcastJoinTop1000JoinMapred.java`

`$ jar cf BroadcastJoinTop1000JoinMapred.jar *.class`

`$ rm *.class`

Run broadcast join job

`$ hadoop-3.1.2/bin/hadoop fs -put top1000.txt /user/tg/`

`$ hadoop-3.1.2/bin/hadoop jar BroadcastJoinTop1000JoinMapred.jar BroadcastJoinTop1000JoinMapred -Dmapreduce.map.memory.mb=1000 -Dmapreduce.map.java.opts.max.heap=800 -Dmapreduce.reduce.memory.mb=1000 -Dmapreduce.reduce.java.opts.max.heap=800 -Dmapreduce.job.reduces=4 /user/tg/input/clickstream-enwiki-2019-04.tsv /user/tg/broadcastJoinedMapRed top1000.txt`

`$ hadoop-3.1.2/bin/hadoop fs -text /user/tg/broadcastJoinedMapRed/* > topJoined.txt`

### Step 4 (optional)

Sort obtained on previous step results

`$ hadoop-3.1.2/bin/hadoop jar BroadcastJoinTop1000Sort.jar BroadcastJoinTop1000Sort -Dmapreduce.map.memory.mb=1000 -Dmapreduce.map.java.opts.max.heap=800 -Dmapreduce.reduce.memory.mb=1000 -Dmapreduce.reduce.java.opts.max.heap=800 -Dmapreduce.job.reduces=4 /user/tg/broadcastJoinedMapRed/ /user/tg/broadcastJoinedMapRedSorted`

`$ hadoop-3.1.2/bin/hadoop fs -text /user/tg/broadcastJoinedMapRedSorted/* > topJoinedSorted.txt`



## Links

[1] Hadoop with [Intellij IDEA](https://bigdataproblog.wordpress.com/2016/05/20/developing-hadoop-mapreduce-application-within-intellij-idea-on-windows-10/)

[2] [MapReduce Tutorial](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Source_Code)

