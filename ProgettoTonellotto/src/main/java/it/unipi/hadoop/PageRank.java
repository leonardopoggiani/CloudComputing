package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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

        if (otherArgs.length != 4) {
            System.err.println("Usage: PageRank <#iterations> <input> <output>");
            System.exit(1);
        }

        Path input = new Path(otherArgs[2]);
        Path output = new Path(otherArgs[3]);
        System.out.println("args[0]: <#iterations>=" + otherArgs[1]);
        System.out.println("args[1]: <input>=" + otherArgs[2]);
        System.out.println("args[2]: <output>=" + otherArgs[3]);

        // set number of iterations
        int iterations = Integer.parseInt(otherArgs[1]);


        FileSystem fs = FileSystem.get(output.toUri(),conf);
        if (fs.exists(output)) {
            System.out.println("Delete old output folder: " + output);
            fs.delete(output, true);
        }


        // for (int i = 0; i < iterations; i++) {

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
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            // define reducer's output key-value
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            // define I/O
            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.waitForCompletion(true);

            //Non ho capito perchè
            //fs.delete(output, true);
        }

    // }

}
