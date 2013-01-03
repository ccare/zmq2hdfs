package me.ccare.hadoop.zmq2hdfs;

import java.io.IOException;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

public class Sink implements Runnable {
	
	private final String address;
	private final HdfsAppender appender;
	private boolean running;
	
	public Sink(final String address, HdfsAppender appender) {
		this.address = address;
		this.appender = appender;
		this.running = true;
	}

	public void run() {
		Context zmq = ZMQ.context(1);
		Poller poller = zmq.poller(1);
		Socket incoming = zmq.socket(ZMQ.SUB);
		poller.register(incoming);
		incoming.bind(address);
		incoming.subscribe("".getBytes());
		System.out.println("Listening on " + address);
		while (true) {
			poller.poll(1000);
			if (poller.pollin(0)) {
				byte[] recv = incoming.recv(0);
				try {
					appender.append(recv);
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else if (!running) {
				return;
			} 
		}
	}
    
	public void shutdown() {
		running = false;
	}
	
}
