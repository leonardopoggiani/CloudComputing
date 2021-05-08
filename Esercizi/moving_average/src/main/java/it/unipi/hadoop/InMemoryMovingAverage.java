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


public class InMemoryMovingAverage
{
    public static class MovingAverageMapper extends Mapper<LongWritable, Text, Text, TimeSeriesData>
    {
        private final static TimeSeriesData tsd = new TimeSeriesData();
        private final Text name = new Text();

        public void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {
            String[] splitRow = value.toString().split(",");
            // [0] -> name, [1] -> date, [2] -> value

            name.set(splitRow[0]);
            Date date = DateUtil.getDate(splitRow[1]);
            assert date != null;
            tsd.setTimestamp(date.getTime());
            tsd.setValue(Double.parseDouble(splitRow[2]));

            context.write(name, tsd);

        }
    }

    public static class MovingAverageReducer extends Reducer<Text, TimeSeriesData, Text, Text> {

        public void reduce(final Text key, final Iterable<TimeSeriesData> values, final Context context)
                throws IOException, InterruptedException {

            /*
            values--   Google,tsd       Gooogle,tsd    Gooogle, tsd
            testData =tsd.getValue() tsd.getValue....
             */

            List<TimeSeriesData> testData = new ArrayList<>();
            for (final TimeSeriesData val : values) {
                testData.add(new TimeSeriesData(val.getValue(),val.getTimestamp()));
            }

            Collections.sort(testData); // [0] il piú recente

            double sum = 0;
            int windowSize = 2;
            for (int i = 0; i < windowSize - 1; i++) {
                sum += testData.get(i).getValue();
            }

            Text result = new Text();

            for (int i = windowSize - 1; i < testData.size(); i++ ) {
                sum += testData.get(i).getValue();
                double moving_average = sum/ windowSize;

                result.set( DateUtil.getDateAsString(testData.get(i).getTimestamp()) + ", " + moving_average );
                context.write(key, result);

                sum -= testData.get(i - windowSize + 1).getValue();
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf, "InMemoryMovingAverage");
        job.setJarByClass(InMemoryMovingAverage.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TimeSeriesData.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MovingAverageMapper.class);
        job.setReducerClass(MovingAverageReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}