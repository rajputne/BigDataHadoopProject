/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bigdataproject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import javax.xml.stream.XMLInputFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author User
 */
public class Driver {

    /**
     * @param args the command line arguments
     */
    static int count = 0;

    public static void main(String[] args) {
        // TODO code application logic here

        try {

            Configuration conf = new Configuration();

            final File f = new File(Driver.class.getProtectionDomain().getCodeSource().getLocation().getPath());
            String inFiles = f.getAbsolutePath().replace("/build/classes", "") + "/src/InputFiles/Sample.dat";
            String outFiles = f.getAbsolutePath().replace("/build/classes", "") + "/src/outputFiles/Result";
            //conf.setInputFormat();
            Job job = new Job(conf, "Twitter Processing");

            job.setJarByClass(Driver.class);
            job.setMapperClass(ExtractTweetsMapper.class);
            job.setReducerClass(ExtractTweetReducer.class);
            job.setNumReduceTasks(1);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //  FileInputFormat.addInputPath(job, new Path(inFiles));
            FileInputFormat.addInputPath(job, new Path(inFiles));
            FileOutputFormat.setOutputPath(job, new Path(outFiles));
            //job.setReducerClass(XMLReducer.class);
            job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
            job.waitForCompletion(true);
            job.killJob();

        } catch (Exception e) {
            System.out.println(e.getMessage().toString());
        }
    }

}
