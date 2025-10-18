package edu.ensias.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.w3c.dom.Text;

/**
 * Main class that launches the MapReduce job
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // Validate arguments
        if (args.length != 2) {
            System.err.println("Usage: WordCount <input-path> <output-path>");
            System.exit(1);
        }
        // Hadoop configuration
        Configuration conf = new Configuration();
        
        // Create MapReduce job
        Job job = Job.getInstance(conf, "word count");
        
        // Configure job
        configureJob(job, args);
        
        // Execute job and return status
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
    
    /**
     * Configure the MapReduce job
     */
    private static void configureJob(Job job, String[] args) throws IOException {
        // Main class
        job.setJarByClass(WordCount.class);
        
        // MapReduce processing classes
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        
        // Output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
    }
}