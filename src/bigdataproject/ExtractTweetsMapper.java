package bigdataproject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;
import org.jsoup.*;
import org.jsoup.helper.*;
import org.jsoup.nodes.*;
import org.jsoup.select.*;

import java.io.*; // Only needed if scraping a local File.
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExtractTweetsMapper extends Mapper<LongWritable, Text, Text, Text> {

    static HashSet hashTags = new HashSet();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //
        String myKey = "";
        String myValue = "";
      
        Pattern p = Pattern.compile("\"([^\"]*)\"");
        Matcher m = p.matcher(value.toString());
        while (m.find()) {

      
            //Capturing English Tweets
            if (value.toString().contains("\"EN\"")) {
                if (m.group(1).contains("https://twitter")) {
                    System.out.print(m.group(1));
                    myKey = m.group(1);
                    Document doc = null;

                    try {
                        doc = (Document) Jsoup.connect(m.group(1)).get();
                    } catch (IOException ioe) {
                        //ioe.printStackTrace();
                    } catch (NullPointerException e) {
                        // TODO Auto-generated catch block
                        //e.printStackTrace();
                    }

                    try {
                        Elements table = doc.getElementsByClass("TweetTextSize TweetTextSize--26px js-tweet-text tweet-text");
                        Elements rows = table.tagName("p");

                        for (Element row : rows) {
                            Elements tds = row.getElementsByTag("p");
                            for (Element r : tds) {
                                myValue = r.text();
                                System.out.println(r.text());
                                if (r.text().contains("#")) {
                                    Pattern MY_PATTERN = Pattern.compile("#(\\S+)");
                                    Matcher mat = MY_PATTERN.matcher(r.text());
                                    myValue += "\t Hash";
                                    while (mat.find()) {
                                        //System.out.println(mat.group(1));
                                        myValue += mat.group();
                                        hashTags.add(mat.group(1));
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        myValue = "Tweet Deleted";
                        System.out.println("Tweet Deleted");
                    }
                }
            }
        }
        context.write(new Text(myKey), new Text(myValue));
    }

}
