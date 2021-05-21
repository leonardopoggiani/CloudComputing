package it.unipi.hadoop;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank
{
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: PageRank <#iterations> <input> <output>");
            System.exit(1);
        }

        Path input = new Path(otherArgs[1]);
        Path output = new Path(otherArgs[2]);
        System.out.println("args[0]: <#iterations>=" + otherArgs[0]);
        System.out.println("args[1]: <input>=" + otherArgs[1]);
        System.out.println("args[2]: <output>=" + otherArgs[2]);

        // set number of iterations
        int iterations = Integer.parseInt(otherArgs[0]);

        FileSystem fs = FileSystem.get(output.toUri(),conf);
        if (fs.exists(output)) {
            System.out.println("Delete old output folder: " + output.toString());
            fs.delete(output, true);
        }

        for (int i = 0; i < iterations; i++) {

            Job job = Job.getInstance(conf, "PageRank");
            job.setJarByClass(PageRank.class);


            //se dobbiamo passare qualche altro parametro
            //job.getConfiguration().setInt("", );


            // set mapper/combiner/reducer
            job.setMapperClass(PageRankMapper.class);
            //job.setCombinerClass(PageRankCombiner.class);
            //job.setPartitionerClass(PageRankPartitioner.class);
            job.setReducerClass(PageRankReducer.class);

            //Da decidere
            job.setNumReduceTasks(3);

            // define mapper's output key-value
            //job.setMapOutputKeyClass(.class);
            //job.setMapOutputValueClass(.class);

            // define reducer's output key-value
            //job.setOutputKeyClass(.class);
            //job.setOutputValueClass(.class);

            // define I/O
            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.waitForCompletion(true);

            //Non ho capito perchè
            //fs.delete(output, true);
        }

    }

}
