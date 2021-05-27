package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InitialRankReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text doc_title, Iterable<Text> list, Context context)
            throws IOException, InterruptedException {

        Configuration conf_for_count = context.getConfiguration();
        double total_doc = Double.parseDouble(conf_for_count.get("totalpages_in_wiki"));
        StringBuilder outlink_list = new StringBuilder();
        StringBuilder s = new StringBuilder();

        for (Text outlink : list) {
            outlink_list.append(outlink).append(";");
        }

        double initial_rank = 1 / total_doc;

        s.append(outlink_list);
        s.append("#####");
        s.append(initial_rank);
        context.write(doc_title,new Text(s.toString()));
    }
}
