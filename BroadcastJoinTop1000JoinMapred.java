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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;

/*
 * https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Source_Code
 * To compile and run the program use:
 * $ export JAVA_HOME=/usr/java/default
 * $ export PATH=${JAVA_HOME}/bin:${PATH}
 * $ export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
 * $ cd compile/BroadcastJoinTop1000JoinMapred/
 * $ javac -cp ~/hadoop-3.1.2/share/hadoop/client/*:~/hadoop-3.1.2/share/hadoop/common/* BroadcastJoinTop1000JoinMapred.java
 * $ jar cf BroadcastJoinTop1000JoinMapred.jar *.class
 * $ hadoop-3.1.2/bin/hadoop fs -put top1000.txt /user/tg/
 * $ hadoop-3.1.2/bin/hadoop jar BroadcastJoinTop1000JoinMapred.jar BroadcastJoinTop1000JoinMapred -Dmapreduce.map.memory.mb=1000 -Dmapreduce.map.java.opts.max.heap=800 -Dmapreduce.reduce.memory.mb=1000 -Dmapreduce.reduce.java.opts.max.heap=800 -Dmapreduce.job.reduces=4 /user/tg/input/clickstream-enwiki-2019-04.tsv /user/tg/broadcastJoinedMapRed top1000.txt
 * $ hadoop-3.1.2/bin/hadoop fs -text /user/tg/broadcastJoinedMapRed/* | head -n1000 > top1000Joined.txt
 */

public class BroadcastJoinTop1000JoinMapred extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(), BroadcastJoinTop1000JoinMapred.class.getCanonicalName());

        job.setJar("BroadcastJoinTop1000JoinMapred.jar");
        job.setJarByClass(BroadcastJoinTop1000JoinMapred.class);

        job.setMapperClass(BroadcastJoinTop1000JoinMapred.TopJoinMapper.class);
        job.setCombinerClass(BroadcastJoinTop1000JoinMapred.TopJoinReducer.class);
        job.setReducerClass(BroadcastJoinTop1000JoinMapred.TopJoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

//        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        job.addCacheFile(new Path(strings[2]).toUri());

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static class TopJoinMapper extends Mapper<Object, Text, Text, LongWritable> {

        private LongWritable count = new LongWritable();
        private Text tag = new Text();
        private final String separator = "\t";
        private final String delimeter = "#";
        private HashSet<String> leftSide = new HashSet<String>();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            super.map(key, value, context);
            String[] parts = value.toString().split(separator);
            String line = parts[1].trim() + delimeter + parts[2].trim();
            if (leftSide.contains(line)) {
                tag.set(line);
                count.set(Integer.parseInt(parts[3].trim()));
                context.write(tag, count);
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
//            super.setup(context);
            URI[] leftSideFiles = context.getCacheFiles();
            for (URI lefSideFile: leftSideFiles) {
                readFile(new Path(lefSideFile));
            }
        }

        private void readFile(Path filePath) {
            try {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    leftSide.add(line.split(separator)[1]);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class TopJoinReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);
            int sum = 0;
            for (LongWritable value: values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) {
        int res = 0;
        try {
            res = ToolRunner.run(new BroadcastJoinTop1000JoinMapred(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(res);
    }
}
