package edu.ensias.hadoop.hdfslab;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {
        /* 
         * this class is for retuning a file status,
         *  it gives info about the file size,name, owner...
         */
        if (args.length < 3) {
            System.err.println("add args: HadoopFileStatus <file_path> <file_name> <new_file_name>");
            System.exit(1);
        }

        String dirPath = args[0];          
        String fileName = args[1];         
        String newFileName = args[2];      

        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);

            Path filePath = new Path(dirPath, fileName);

            if (!fs.exists(filePath)) {
                System.out.println("File does not exist: " + filePath);
                System.exit(1);
            }

            FileStatus infos = fs.getFileStatus(filePath);

            // Infos fichier
            System.out.println("File Name: " + filePath.getName());
            System.out.println("File Size: " + infos.getLen());
            System.out.println("File owner: " + infos.getOwner());
            System.out.println("File permission: " + infos.getPermission());
            System.out.println("File Replication: " + infos.getReplication());
            System.out.println("File Block Size: " + infos.getBlockSize());

            // Infos blocs
            BlockLocation[] blockLocations = fs.getFileBlockLocations(infos, 0, infos.getLen());
            for (BlockLocation blockLocation : blockLocations) {
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                for (String host : blockLocation.getHosts()) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }

            // Renommer fichier
            Path newFilePath = new Path(dirPath, newFileName);
            if (fs.rename(filePath, newFilePath)) {
                System.out.println("File successfully renamed to: " + newFilePath.getName());
            } else {
                System.out.println("Failed to rename file");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
