package me.ccare.hadoop.zmq2hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;

public class HdfsAppender {

	private FSDataOutputStream output;
	
	public HdfsAppender(final FSDataOutputStream output) throws IOException {
		this.output = output;
	}

	public void append(byte[] data) throws IOException {
		output.write(data);
		output.hsync();
	}
	
	public void close() throws IOException {
		output.flush();
		output.close();
	}

}
