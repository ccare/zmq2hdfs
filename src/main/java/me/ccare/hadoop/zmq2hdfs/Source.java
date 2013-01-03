package me.ccare.hadoop.zmq2hdfs;

import java.util.Date;
import java.util.Random;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public class Source implements Runnable {
	
	private final String address;
	private final Random random = new Random();
	
	public Source(final String address) {
		this.address = address;
	}

	public void run() {
		Context zmq = ZMQ.context(1);
		Socket outbound = zmq.socket(ZMQ.PUB);
		outbound.connect(address);
		System.out.println("Connected to " + address);
		for (int i = 0; i < 100; i++) {
			String msg = String.format("Line %d: created at at %s with random data: %s\n", i, new Date(), random.nextLong());
			outbound.send(msg.getBytes(), 0);
			try {
				Thread.sleep(10l);
			} catch (InterruptedException e) {
				// Ignore for dummy app
			}
		}
		outbound.setLinger(0);
		outbound.close();
		zmq.term();
	}

}
