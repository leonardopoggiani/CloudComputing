package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException
    {
        String line = value.toString();

    }
}