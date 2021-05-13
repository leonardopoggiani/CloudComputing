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
import org.apache.hadoop.util.GenericOptionsParser;


public class MatrixMultiplication
{
    public static class MatrixMultiplicationMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final static Text outputKey = new Text();
        private final static Text outputValue = new Text();

        private final int columnsN = 100;
        private final int rowsM = 100;

        public void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {
            String[] splittedRow = value.toString().split(",");
            // [0] -> nome matrice, [1] -> numero riga (i), [2] -> numero colonna (j), [3] -> valore

            if (splittedRow[0].equals("M")) {
                for(int k = 0; k < columnsN; k++) {
                    outputKey.set(splittedRow[1] + "," + k);
                    outputValue.set(splittedRow[0] + "," + splittedRow[2] + "," + splittedRow[3]);
                    context.write(outputKey, outputValue);
                }
            } else {
                for(int i = 0; i < rowsM; i++) {
                    outputKey.set(i + "," + splittedRow[2]);
                    outputValue.set(splittedRow[0] + "," + splittedRow[1] + "," + splittedRow[3]);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    public static class MatrixMultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(final Text key, final Iterable<Text> values, final Context context)
                throws IOException, InterruptedException {

            //  key -> (2,3) | (M, *, *) .. (N, *, *)

            Map<Integer, Float> hashA = new HashMap<Integer, Float>();
            Map<Integer, Float> hashB = new HashMap<Integer, Float>();

            String[] arrayMat;
            for(Text value : values) {
                arrayMat = value.toString().split(",");
                if(arrayMat[0].equals("M")){
                    hashA.put(Integer.parseInt(arrayMat[1]),Float.parseFloat(arrayMat[2]));
                } else {
                    hashB.put(Integer.parseInt(arrayMat[1]),Float.parseFloat(arrayMat[2]));
                }
            }

            float sum = 0;
            float mij;
            float njk;
            for(int j = 0; j < 1000; j++) {
                mij = hashA.containsKey(j) ? hashA.get(j) : 0;

                njk  = hashB.containsKey(j) ? hashB.get(j) : 0;

                sum += mij * njk;
            }

            Text result = new Text();
            result.set(String.valueOf(sum));

            if(sum != 0)
                context.write(key, result);
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

        System.out.println(args);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
