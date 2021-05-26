package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class CountPagesMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    int num_pages = 0;
    IntWritable valueEmit = new IntWritable();
    Text keyEmit = new Text("Total Lines");

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        valueEmit.set(num_pages);
        context.write(keyEmit,valueEmit);
    }

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) {
        num_pages += 1;
    }
}
