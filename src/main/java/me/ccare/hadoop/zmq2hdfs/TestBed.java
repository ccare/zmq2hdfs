package me.ccare.hadoop.zmq2hdfs;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class TestBed {

    private static final String QUEUE_ADDRESS = "tcp://*:9595";

    public static void main( String[] args ) throws InterruptedException, IOException {
    	// Create a local HDFS
    	Configuration conf = new Configuration();
      	MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		MiniDFSCluster hdfsCluster = builder.numDataNodes(1).build();
		DistributedFileSystem hdfs = hdfsCluster.getFileSystem();
		// Create a source of data
		Thread sourceThread = new Thread(new Source(QUEUE_ADDRESS));		
		// Create the sink thread which will write to HDFS
		Path filePath = new Path("/capturedData");
		FSDataOutputStream output = hdfs.create(filePath, true);	
		HdfsAppender appender = new HdfsAppender(output);    	
        Sink sink = new Sink(QUEUE_ADDRESS, appender);
		Thread sinkThread = new Thread(sink);
        sinkThread.start();

        // Start, and drain, the source
        System.out.println(" ==== Appending ==== ");
		sourceThread.start();
		sourceThread.join();     
        
		// Shutdown the sink and HDFS appender
        sink.shutdown();
        sinkThread.join();
        appender.close();
        
        // Read the data back out of HDFS
        System.out.println(" ==== Reading ==== ");
        FSDataInputStream in = hdfs.open(filePath);
        IOUtils.copy(in, System.out);
        
        // Shutdown the mini-hdfs
        hdfsCluster.shutdown();
    }
}
