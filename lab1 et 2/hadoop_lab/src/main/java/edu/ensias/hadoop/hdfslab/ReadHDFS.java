package edu.ensias.hadoop.hdfslab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadHDFS {

    public static void main(String[] args) throws IOException {
        /*
         * This class reads a file from HDFS.
         * It prints the content of the file line by line.
         * The file path on HDFS is passed as a command-line argument.
         */

        // Check if a file path is provided
        if (args.length < 1) {
            System.err.println("Usage: hadoop jar ReadHDFS.jar <hdfs_file_path>");
            System.exit(1);
        }

        String filePathArg = args[0]; // HDFS file path provided by the user

        // Hadoop configuration
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Path object representing the file on HDFS
        Path filePath = new Path(filePathArg);

        // Open the file for reading
        FSDataInputStream inputStream = fs.open(filePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        // Read the file line by line and print each line
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        // Close all resources
        reader.close();
        inputStream.close();
        fs.close();
    }
}
