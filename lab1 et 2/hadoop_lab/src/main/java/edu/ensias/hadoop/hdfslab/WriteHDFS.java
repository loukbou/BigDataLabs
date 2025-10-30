package edu.ensias.hadoop.hdfslab;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteHDFS {

    public static void main(String[] args) throws IOException {
        /*
         * writes a file to HDFS
         */
        if (args.length < 2) {
            System.err.println("Usage: hadoop jar WriteHDFS.jar <file_path> <file_content>");
            System.exit(1);
        }

        // Configuration Hadoop
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // path new file in HDFS
        Path filePath = new Path(args[0]);

        if (!fs.exists(filePath)) {
            FSDataOutputStream outStream = fs.create(filePath);
            outStream.writeUTF(args[1]);  // Texte passé en argument
            outStream.close();
            System.out.println("file created in HDFS : " + filePath.toString());
        } else {
            System.out.println("file already exists: " + filePath.toString());
        }

        fs.close();
    }
}
