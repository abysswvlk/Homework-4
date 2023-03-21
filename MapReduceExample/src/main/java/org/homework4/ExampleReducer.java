package org.homework4;


import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ExampleReducer  extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values,OutputCollector<Text,IntWritable> output,
                       Reporter reporter) throws IOException {
          /*
                A reducer function that takes the sum of each word's counts from all of the mappers'
                intermediate outputs and outputs a key-value pair with the key being a word from
                the input text and the value being the sum of those numbers.
                Inputs:
                    -key: The input key for the reducer function, which represents a word in
                    the input text.
                    -values: An iterator object that allows iterating over the values associated
                    with a given key.
                    -output: The output collector that collects the final output of the reducer function.
                    -reporter: A reporter object that can be used to report progress and update
                    status information.
                Constrains:
                    -The input key must be a Text object and cannot be null.
                    -The input values must be an iterator object that iterates over IntWritable
                    values and cannot be null.
                    -The output collector must collect output in the form of Text and IntWritable
                    objects and cannot be null.
                    -The reporter object must be an instance of the Reporter class and cannot be null.
                Outputs:
                    -The reducer function outputs a key-value pair where the key is a word in the
                    input text and the value is the sum of the counts of that word from
                    all mapper outputs.
                References:
                    -Code With Arjun. (2022, October 12). MapReduce Word Count Example using Hadoop
                    and Java [Video]. YouTube. https://www.youtube.com/watch?v=qgBu8Go1SyM
         */
        int total=0;
        while (values.hasNext()) {
            total+=values.next().get();
        }
        output.collect(key,new IntWritable(total));
    }
}