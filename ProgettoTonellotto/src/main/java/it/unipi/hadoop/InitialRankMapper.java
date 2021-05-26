package it.unipi.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InitialRankMapper extends Mapper<LongWritable, Text, Text, Text>
{
    private static final Pattern title_pat = Pattern.compile("<title>(.*?)</title>");
    private static final Pattern text_pat = Pattern.compile(".*<text.*?>(.*?)</text>.*");
    private static final Pattern link_pat = Pattern.compile("\\[\\[(.*?)]]");

    @Override
    public void map(LongWritable offset, Text input, Context context)
            throws IOException, InterruptedException
    {

        String line = input.toString();

        Matcher title_matcher = title_pat.matcher(line);
        Matcher text_matcher = text_pat.matcher(line);

        if(title_matcher.find())
        {
            while(text_matcher.find())
            {
                String text = text_matcher.group(1);
                Matcher link_matcher = link_pat.matcher(text);

                while(link_matcher.find())
                {
                    String link = getWikiPageFromLink(link_matcher.group(1));

                    if( link != null ) {
                        Text url = new Text(title_matcher.group(1));
                        Text links = new Text(link);
                        context.write(url,links);
                    }

                }

            }

        }

    }

    private String getWikiPageFromLink(String aLink){
        if(isInvalidWikiLink(aLink)) return null;

        int start = 0;
        int endLink = aLink.length();

        int pipePosition = aLink.indexOf("|");
        if(pipePosition > 0){
            endLink = pipePosition;
        }

        int part = aLink.indexOf("#");
        if(part > 0){
            endLink = part;
        }

        aLink =  aLink.substring(start, endLink);
        aLink = aLink.replaceAll("\\s", "_");
        aLink = aLink.replaceAll(",", "");
        aLink = (aLink.contains("&amp;"))? aLink.replace("&amp;", "&"): aLink;

        return aLink;
    }

    private boolean isInvalidWikiLink(String aLink) {
        int minLength = 1;
        int maxLength = 100;


        if( aLink.length() < minLength+2 || aLink.length() > maxLength) return true;
        char firstChar = aLink.charAt(minLength);

        if( firstChar == '#') return true;
        if( firstChar == ',') return true;
        if( firstChar == '.') return true;
        if( firstChar == '&') return true;
        if( firstChar == '\'') return true;
        if( firstChar == '-') return true;
        if( firstChar == '{') return true;

        if( aLink.contains(":")) return true;
        if( aLink.contains(",")) return true;
        return (aLink.indexOf('&') > 0) && !(aLink.substring(aLink.indexOf('&')).startsWith("&amp;"));
    }
}
