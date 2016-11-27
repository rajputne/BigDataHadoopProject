// 
// Author - Jack Hebert (jhebert@cs.washington.edu) 
// Copyright 2007 
// Distributed under GPLv3 
// 
// Modified - Dino Konstantopoulos 
// Distributed under the "If it works, remolded by Dino Konstantopoulos, 
// otherwise no idea who did! And by the way, you're free to do whatever 
// you want to with it" dinolicense
// 
package bigdataproject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ExtractTweetReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String myValue = "";
        for (Text value : values) {
            myValue = value.toString();
            
            if (myValue.contains("Hash")) {
                String hashTagSeparator[] = myValue.split("Hash");
                String cleartext = hashTagSeparator[0].replaceAll("http.*?\\s", " ");
                context.write(new Text(cleartext), new Text(hashTagSeparator[1]));
               // context.write(key, value);
            }
        }
    }
}
