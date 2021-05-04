package it.unipi.hadoop;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class InMemoryMovingAverage
{
    public static class NewMapper extends Mapper<Object, Text, Text, TimeSeriesData>
    {
        private final static TimeSeriesData tsd = new TimeSeriesData();
        private final Text name = new Text();

        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            String[] splittedRow = value.toString().split(",");
            // [0] -> name, [1] -> date, [2] -> value

            name.set(splittedRow[0]);
            Date date = new Date(splittedRow[1]);
            tsd.setTimestamp(date.getTime());
            tsd.setValue(Double.parseDouble(splittedRow[2]));

            context.write(name, tsd);

        }
    }

    public static class NewReducer extends Reducer<Text, TimeSeriesData, Text, Text> {
        private final int windowSize = 4;

        public void reduce(final Text key, final Iterable<TimeSeriesData> values, final Context context)
                throws IOException, InterruptedException {

            /*
            values--   Google,tsd       Gooogle,tsd    Gooogle, tsd
            testData =tsd.getValue(,) tsd.getValue....

             */
            ArrayList testData = new ArrayList<TimeSeriesData>();
            for (final TimeSeriesData val : values) {
                testData.add(new TimeSeriesData(val.getValue(),val.getTimestamp()));
            }

            Collections.sort(testData); // [0] il piú recente

            double sum = 0;
            for (int i = 0; i < windowSize - 1; i++) {
                sum += ((TimeSeriesData)testData.get(i)).getValue();
            }

            Text result = new Text();

            for (int i = windowSize - 1; i < testData.size(); i++ ) {
                sum += ((TimeSeriesData)testData.get(i)).getValue();
                double moving_average = sum/windowSize;

                Date date = new Date(((TimeSeriesData) testData.get(i)).getTimestamp());
                result.set(date.toString() + ", " + moving_average);
                context.write(key, result);

                // windowSize = 4, index = 4, togliere 0,  5 - 4
                // index = 6, togliere 2
                // index = 7, togliere 3
                sum -= ((TimeSeriesData)testData.get(i - windowSize + 1)).getValue();
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final Job job = new Job(conf, "moving_average");
        job.setJarByClass(InMemoryMovingAverage.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(NewMapper.class);
        job.setReducerClass(NewReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}