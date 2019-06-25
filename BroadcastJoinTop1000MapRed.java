import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * $ cd compile/BroadcastJoinTop1000MapRed/
 * $ javac -cp ~/hadoop-3.1.2/share/hadoop/client/*:~/hadoop-3.1.2/share/hadoop/common/* BroadcastJoinTop1000MapRed.java
 * $ jar cf BroadcastJoinTop1000MapRed.jar *.class
 * $ hadoop-3.1.2/bin/hadoop jar BroadcastJoinTop1000MapRed.jar BroadcastJoinTop1000MapRed -Dmapreduce.map.memory.mb=1000 -Dmapreduce.map.java.opts.max.heap=800 -Dmapreduce.reduce.memory.mb=1000 -Dmapreduce.reduce.java.opts.max.heap=800 -Dmapreduce.job.reduces=4 /user/tg/input/clickstream-enwiki-2019-03.tsv /user/tg/output/
 */


public class BroadcastJoinTop1000MapRed extends Configured implements Tool {
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), BroadcastJoinTop1000MapRed.class.getCanonicalName());
        job.setJar("BroadcastJoinTop1000MapRed.jar");
        job.setJarByClass(BroadcastJoinTop1000MapRed.class);
        job.setMapperClass(TopMapper.class);
        job.setCombinerClass(TopReducer.class);
        job.setReducerClass(TopReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        return job.waitForCompletion(true)? 0: 1;
    }

    public static class TopMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final String separator = "\t";
        private Text tag = new Text();
        private IntWritable count = new IntWritable();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            super.map(key, value, context);
            String[] line = value.toString().split(separator);
            tag.set(line[1].trim() + "#" + line[2].trim());
            count.set(Integer.parseInt(line[3].trim()));
            context.write(tag, count);
        }
    }

    public static class TopReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) {
        int res = 0;
        try {
            res = ToolRunner.run(new BroadcastJoinTop1000MapRed(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(res);
    }
}
