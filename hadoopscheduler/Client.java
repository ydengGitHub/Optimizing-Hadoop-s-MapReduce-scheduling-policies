package hadoopscheduler;

import java.io.BufferedReader;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * @author Yan Deng 04/16/2016
 */

public class Client {
	public static final String IP_ADDR = "localhost";
	public static final int PORT = 12345;

	public static void main(String[] args) {
		System.out.println("client start...");
		String[] tasks = null;
		int num = 0;
		if (args.length != 0) {
			num=Integer.parseInt(args[0]);
			//System.out.println("Num: "+num+" Length: "+args.length);
			tasks=new String[num];
			for(int i=0;i<num;i++){
				tasks[i]=args[i*4+1]+" "+args[i*4+2]+" "+args[i*4+3]+" "+args[i*4+4];
			}
			
		} else {
			try {
				System.out
						.print("Please enter how many clients do we have: \t");
				String str1 = new BufferedReader(new InputStreamReader(
						System.in)).readLine();
				num = Integer.valueOf(str1);
				tasks = new String[num];
				for (int i = 0; i < num; i++) {
					System.out
							.print("Please enter Client index,period,running time,input folder: \t");
					tasks[i] = new BufferedReader(new InputStreamReader(
							System.in)).readLine();
				}
			} catch (Exception e) {
				System.out.println("client error: Invalid input.");
			}
		}
		AtomicInteger sharedCount = new AtomicInteger(0);
		Thread t1 = new Thread(new DoSocket(sharedCount, tasks[0]));
		Thread t2 = null;
		Thread t3 = null;
		t1.start();
		if (num >= 2) {
			try {
				Thread.sleep(8000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			t2 = new Thread(new DoSocket(sharedCount, tasks[1]));
			t2.start();
		}
		if (num >= 3) {
			try {
				Thread.sleep(8000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			t3 = new Thread(new DoSocket(sharedCount, tasks[2]));
			t3.start();
		}
		try {
			t1.join();
			if (num >= 2) {
				t2.join();
			}
			if (num >= 3) {
				t3.join();
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}

class DoSocket implements Runnable {
	private final AtomicInteger counter;
	private String str;
	private int clientIndex;
	private int period;
	//private int runTime;
	//private String inputFolder;

	DoSocket(AtomicInteger counter, String str) {
		this.counter = counter;
		this.str = str;
		String[] arguments=str.split(" ");
		this.clientIndex = Integer.parseInt(arguments[0]);
		this.period = Integer.parseInt(arguments[1]);
		//this.runTime=Integer.parseInt(arguments[2]);
		//this.inputFolder=arguments[3];
	}

	public void run() {
		InetAddress address = null;
		try {
			address = InetAddress.getLocalHost();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		// ServerSocket serverSocket = null;
		Socket socket = null;
		// DataOutputStream out =null;
		String line = null;
		BufferedReader is = null;
		PrintWriter os = null;
		String response = null;
		try {
			// serverSocket = new ServerSocket(Client.PORT);
			socket = new Socket(address, Client.PORT);
			is = new BufferedReader(new InputStreamReader(
					socket.getInputStream()));
			os = new PrintWriter(socket.getOutputStream());
			/*
			 * out = new DataOutputStream( socket.getOutputStream());
			 */
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			while (counter.getAndAdd(1) < 300) {
				System.out.println("Count: " + counter.get());
				System.out.println("Client" + clientIndex + " is sending request...");
				os.println(str);
				os.flush();
				response = is.readLine();
				System.out.println("Server Response: " + response);
				// out.writeUTF(str);
				// out.close();
				try {
					Thread.sleep(period);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			System.out.println("client error:" + e.getMessage());
		} finally {
			if (socket != null) {
				try {
					socket.close();
					is.close();
					os.close();
					System.out.println("Connection Closed.");
				} catch (IOException e) {
					socket = null;
					System.out
							.println("client finally error:" + e.getMessage());
				}
			}
		}
	}
}