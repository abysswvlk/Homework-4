package org.homework4;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
public class ExampleRunner {
    public static void main(String[] args) throws IOException{
        /*
            MapReduce job runner for a Word Count program, which takes input data and counts
            the frequency of each word, then returns the output in the format of word-frequency pairs.
            Inputs:
                -args: an array of command-line arguments containing two strings, the input file path
                and the output directory path.
            Constraints:
                -The input file must be in text format.
                -The output directory must not already exist.
            Outputs:
                -A set of output files in the specified output directory, containing word-frequency pairs.
            References:
                -Code With Arjun. (2022, October 12). MapReduce Word Count Example using Hadoop
                and Java [Video]. YouTube. https://www.youtube.com/watch?v=qgBu8Go1SyM
        */

        JobConf conf = new JobConf(ExampleRunner.class);

        // Set job name
        conf.setJobName("WordCount");

        // Set output key and value classes
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        // Set mapper, combiner and reducer classes
        conf.setMapperClass(ExampleMapper.class);
        conf.setCombinerClass(ExampleReducer.class);
        conf.setReducerClass(ExampleReducer.class);

        // Set input and output formats
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        // Set input and otput file paths
        FileInputFormat.setInputPaths(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));

        //Run job
        JobClient.runJob(conf);
    }
}