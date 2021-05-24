package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    public static int numPages = 0;
    private static final Pattern title_pat = Pattern.compile("<title>(.*?)</title>");
    private static final Pattern text_pat = Pattern.compile(".*<text.*?>(.*?)</text>.*");
    private static final Pattern link_pat = Pattern.compile("\\[\\[(.*?)\\]\\]");

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException
    {
        String line = value.toString();

        if(!line.isEmpty()) {
            Text pageName = new Text();
            Matcher title1 = title_pat.matcher(line);

            if(title1.find()){
                pageName = new Text(title1.group(1));
                context.write(new Text(pageName), new IntWritable(1));
            }
        }

    }
}