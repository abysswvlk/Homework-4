package org.homework4;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
public class ExampleMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
    /*
        This is a Java class for a mapper in Hadoop's MapReduce framework. It takes in a
        LongWritable key, a Text value, and outputs a Text key and IntWritable value. The map method
        tokenizes the input value, maps each token to a Text key with a count of 1 as the IntWritable
        value, and outputs each key-value pair to the collector.
        Inputs:
            -key: a LongWritable object representing the byte offset of the current input record.
            -value: a Text object representing the current input record.
        Constraints:
            -The input data must be in text format, where each record is a line of text.
            -The input data must be stored in a distributed file system such as
            Hadoop Distributed File System (HDFS).
        Outputs:
            -word: a Text object representing the word extracted from the input record.
            -one: an IntWritable object with a value of 1, used as the output value for each word.
        References:
            -Code With Arjun. (2022, October 12). MapReduce Word Count Example using Hadoop
            and Java [Video]. YouTube. https://www.youtube.com/watch?v=qgBu8Go1SyM
    */
    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text();
    public void map(LongWritable key, Text value,OutputCollector<Text,IntWritable> output,
                    Reporter reporter) throws IOException{
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()){
            word.set(tokenizer.nextToken());
            output.collect(word, one);
        }
    }

}
