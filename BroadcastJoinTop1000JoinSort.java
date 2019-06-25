import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/*
 * https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Source_Code
 * To compile and run the program use:
 * $ export JAVA_HOME=/usr/java/default
 * $ export PATH=${JAVA_HOME}/bin:${PATH}
 * $ export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
 * $ cd compile/BroadcastJoinTop1000JoinSort/
 * $ javac -cp ~/hadoop-3.1.2/share/hadoop/client/*:~/hadoop-3.1.2/share/hadoop/common/* BroadcastJoinTop1000JoinSort.java
 * $ jar cf BroadcastJoinTop1000JoinSort.jar *.class
 * $ hadoop-3.1.2/bin/hadoop jar BroadcastJoinTop1000JoinSort.jar BroadcastJoinTop1000JoinSort -Dmapreduce.map.memory.mb=1000 -Dmapreduce.map.java.opts.max.heap=800 -Dmapreduce.reduce.memory.mb=1000 -Dmapreduce.reduce.java.opts.max.heap=800 -Dmapreduce.job.reduces=4 /user/tg/broadcastJoinedMapRed/ /user/tg/broadcastJoinedMapRedSorted
 * $ hadoop-3.1.2/bin/hadoop fs -text /user/tg/broadcastJoinedMapRedSorted/* > top1000JoinedSorted.txt
 */

public class BroadcastJoinTop1000JoinSort  extends Configured implements Tool {
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), BroadcastJoinTop1000JoinSort.class.getCanonicalName());

        job.setJar("BroadcastJoinTop1000JoinSort.jar");
        job.setJarByClass(BroadcastJoinTop1000JoinSort.class);

        job.setMapperClass(BroadcastJoinTop1000JoinSort.TopSortMapper.class);
        job.setCombinerClass(BroadcastJoinTop1000JoinSort.TopSortReducer.class);
        job.setReducerClass(BroadcastJoinTop1000JoinSort.TopSortReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static class TopSortMapper extends Mapper<Object, Text, LongWritable, Text> {

        private final String separator = "\t";
        private LongWritable count = new LongWritable();
        private Text tag = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            super.map(key, value, context);
            String[] line = value.toString().split(separator);
            count.set(Integer.parseInt(line[1].trim()));
            tag.set(line[0]);
            context.write(count, tag);
        }
    }

    public static class TopSortReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);
            for (Text value: values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) {
        int res = 0;
        try {
            res = ToolRunner.run(new BroadcastJoinTop1000JoinSort(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(res);
    }
}
