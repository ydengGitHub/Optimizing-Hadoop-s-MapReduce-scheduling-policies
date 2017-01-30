package hadoopscheduler;

import java.io.BufferedReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;

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

/**
 * @author Yan Deng 05/03/2016
 */

public class Server {
	public static final int PORT = 12345;
	public static int i = 0;
	public static long initialTime = 0;
	static File file;
	static FileWriter fileWriter;
	static PrintWriter printWriter;
	static Scheduler scheduler;
	static Boolean start = false;

	public static void main(String[] args) {
		System.out.println("Server Start...\n");
		scheduler = new EDF();/* Earliest Deadline Scheduler */
		//scheduler = new SRT();/* Shortest Running Time Scheduler */
		//scheduler=new FIFO();/*First-In-First-Out Scheduler*/
		Server server = new Server();
		server.init();
	}

	public void init() {
		ServerSocket serverSocket = null;
		Socket client = null;
		System.out.println("Server Listening......");
		System.out.println();
		try {
			serverSocket = new ServerSocket(PORT);
		} catch (Exception e) {
			System.out.println("Server error: " + e.getMessage());
		}
		try {
			file = new File("data.txt");
			// fileWriter = new FileWriter(file);
			// fileWriter.write("Job Index\tInput Folder\tClient Index\tRunning Time\tMissed Deadline");
			// fileWriter.write(" ");
			// fileWriter.close();
			printWriter = new PrintWriter(file);
			printWriter
					.println("Job Index\tInput Folder\tClient Index\tWating Time\tMissed Deadline");
			printWriter.close();
		} catch (Exception e) {
		}

		while (true) {
			try {
				client = serverSocket.accept();
				System.out.println("Connection Established.");
				Thread ht = new Thread(new HandlerThread(client));
				ht.start();
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Connection error.");
			}
		}
	}

	/**
	 * Handler Thread, receive client's requests and add them to the scheduler.
	 * 
	 * @author Yan Deng
	 * 
	 */
	class HandlerThread implements Runnable {
		String line = null;
		BufferedReader is = null;
		PrintWriter os = null;
		Socket socket = null;

		public HandlerThread(Socket client) {
			this.socket = client;
			// new Thread(this).start();
		}

		public void run() {
			try {
				is = new BufferedReader(new InputStreamReader(
						socket.getInputStream()));
				os = new PrintWriter(socket.getOutputStream());

				while (true) {
					line = is.readLine();
					System.out.println("Request from Client: " + line);
					String[] content = line.split(" ");
					os.println("Accepted.");
					os.flush();
					System.out
							.println("Response to client : Request Accepted.");

					int index = Integer.valueOf(content[0]);
					int period = Integer.valueOf(content[1]);
					int runningTime = Integer.valueOf(content[2]);
					String inputFolder = content[3];
					synchronized (start) {
						if (start == false) {
							initialTime = System.currentTimeMillis();
							start = true;
							Thread st = new Thread(new SchedulerThread());
							st.start();
						}
					}

					WordCountJob job = new WordCountJob(index, period,
							runningTime, inputFolder);
					synchronized (scheduler) {
						scheduler.add(job);
						System.out.println("Job from client "+index+" was added to te scheduler.");
					}
				}

			} catch (Exception e) {
				System.out.println("client run error: " + e.getMessage());
			} finally {
				if (socket != null) {
					try {
						socket.close();
					} catch (Exception e) {
						socket = null;
						System.out.println("client finally error:"
								+ e.getMessage());
					}
				}
				if (is != null) {
					try {
						is.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				if (os != null) {
					os.close();
				}
			}
		}
	}

	/**
	 * Scheduler Thread: schedule the jobs.
	 * 
	 * @author Yan Deng
	 * 
	 */
	class SchedulerThread implements Runnable {
		private WordCountJob currentJob = null;;
		private WordCountJob nextJob = null;;
		private String output;
		private int waitingTime;

		public SchedulerThread() {
			this.currentJob = this.getJob(scheduler);
		}

		public void run() {
			int counter = 0;
			while (counter < 300) {
				long startTime=System.currentTimeMillis();
				currentJob.startTime=(int) (startTime-initialTime);
				startWordCount();
				nextJob = this.getJob(scheduler);
//				waitingTime = nextJob.startTime
//						- ((int) (System.currentTimeMillis() - initialTime));
				waitingTime=currentJob.runningTime-((int) (System.currentTimeMillis()-startTime));
				if (waitingTime >= 0) {
					try {
						Thread.sleep(waitingTime);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				currentJob = nextJob;
				nextJob=null;
				counter++;
			}
		}

		private void startWordCount() {
			String input = currentJob.inputFolder;
			output = "output/" + i;
			WordCount wc = new WordCount(input, output, i,
					currentJob.clientIndex);
			// wc.setinput(input);
			// wc.setoutput(output);
			wc.setArriveTime(currentJob.arriveTime);
			wc.setDeadline(currentJob.deadline);
			i++;
			System.out.println("Start Job from Client "
					+ currentJob.clientIndex);
			wc.run();
		}

		private WordCountJob getJob(Scheduler sch) {
			WordCountJob job = null;
			while (true) {
				synchronized (scheduler) {
					if (!scheduler.isEmpty()) {
						job = scheduler.getJob();
						// System.out.println("Get job from client "+job.clientIndex);
						break;
					}
				}
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return job;
		}
	}
}
