import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class OrphanPages extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(OrphanPages.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "Orphan Pages");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(LinkCountMap.class);
        job.setReducerClass(OrphanPageReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(OrphanPages.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //TODO
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, ": ");
            //initialize page of this line (should be first token before ':')
            Integer parent = new Integer(-1);
            if (tokenizer.hasMoreTokens()) {
                String p = tokenizer.nextToken();
                parent = Integer.parseInt(p);
                //pass (key: parent page, val: 1)
                context.write(new IntWritable(parent), new IntWritable(1));
            }
            //pass (key: child page, val: 0)
            while (tokenizer.hasMoreTokens()) {
                String nextToken = tokenizer.nextToken();
                Integer child = new Integer(Integer.parseInt(nextToken));
                context.write(new IntWritable(child), new IntWritable(0));
            }
            //context.write(<IntWritable>, <IntWritable>); // pass this output to reducer
        }
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //TODO
            boolean orphan = true;
            for (IntWritable value : values) {
                if (value.get() == 0) {
                    orphan = false;
                }
            }
            if (orphan) {
                context.write(key, NullWritable.get());
            }
            //context.write(<IntWritable>, <NullWritable>); // print as final output
        }
    }
}
