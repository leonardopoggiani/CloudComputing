package it.unipi.hadoop;

import java.io.IOException;
import java.text.DateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MatrixMultiplication
{
    public static class MatrixMultiplicationMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final static Text mapperKey = new Text();
        private final static Text reducerKey = new Text();

        public void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {
            String[] splittedRow = value.toString().split(",");
            // [0] -> nome matrice, [1] -> numero riga (i), [2] -> numero colonna (j), [3] -> valore
            mapperKey.set(splittedRow[1] + "," + splittedRow[2]);
            reducerKey.set(splittedRow[0] + ","  + splittedRow[3]);

            context.write(mapperKey, reducerKey);
        }
    }

    public static class MatrixMultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(final Text key, final Text values, final Context context)
                throws IOException, InterruptedException {

        }
    }

    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf, "MatrixMultiplication");
        job.setJarByClass(MatrixMultiplication.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MatrixMultiplicationMapper.class);
        job.setReducerClass(MatrixMultiplicationReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
