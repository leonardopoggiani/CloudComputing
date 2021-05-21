package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for (IntWritable count : values) {
            sum+=count.get();  //we are summing up all the values of N from different nodes in reduce phase and writing that in the context
        }

        context.getCounter("totalpages_in_wiki","totalpages_in_wiki").increment(sum);
    }
}