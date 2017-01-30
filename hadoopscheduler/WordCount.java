package hadoopscheduler;

import java.io.IOException;
import java.util.Random;
import java.io.File;  
import java.io.FileNotFoundException;  
import java.io.FileOutputStream;  
import java.io.FileWriter;  
import java.io.IOException;  
import java.io.PrintStream;  
import java.io.PrintWriter;  
import java.io.RandomAccessFile; 

import java.io.InputStream;
import java.io.OutputStream;
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
 * @author Yan Deng
 * 04/16/2016
 */

public class WordCount extends Process{
	public static String input;
	public static String output;
	public boolean isfinished = false;
	public int arriveTime;
	public int deadline;
	public int jobIndex;
	public int clientIndex;
	  static File file;
	  static FileWriter fileWriter;
	  static PrintWriter printWriter;
	public WordCount(){
		
	}
	
	public WordCount(String input, String output, int i, int cIndex){
		this.input=input;
		this.output = output;
		this.jobIndex=i;
		this.clientIndex=cIndex;
	}

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public void setinput(String input){
	  this.input = input;
  }
  
  public void setoutput(String output){
	  this.output = output;
  }
  
  public void setArriveTime(int time){
	  this.arriveTime=time;
  }
  
  public void setDeadline(int time){
	  this.deadline=time;
  }

  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public void run()  {
    Configuration conf = new Configuration();
    /*String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }*/
    try{
    	long startTime = System.currentTimeMillis();
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    if(job.waitForCompletion(true)){
    	long endTime = System.currentTimeMillis();
    	System.out.println(endTime-startTime);
    	file = new File("data.txt");
    	fileWriter = new FileWriter(file,true);
        printWriter = new PrintWriter(fileWriter);
        printWriter.println(" ");
        long runningTime=endTime-startTime;
        long waitingTime=startTime-Server.initialTime-arriveTime;
        boolean miss;
        int timeDelay=(int) (endTime-(Server.initialTime+this.deadline));
        if(timeDelay<=0){
        	printWriter.println(jobIndex+"\t"+input+"\t"+clientIndex+"\t"+waitingTime);
        }else{
        	printWriter.println(jobIndex+"\t"+input+"\t"+clientIndex+"\t"+waitingTime+"\tMissed");
        }
        printWriter.close();
        fileWriter.close();
    	
    	
    	
    	isfinished = true;
    	job.killJob();
    }
    }catch(Exception e){}
  }

@Override
public void destroy() {
	// TODO Auto-generated method stub
	
}

@Override
public int exitValue() {
	// TODO Auto-generated method stub
	return 0;
}

@Override
public InputStream getErrorStream() {
	// TODO Auto-generated method stub
	return null;
}

@Override
public InputStream getInputStream() {
	// TODO Auto-generated method stub
	return null;
}

@Override
public OutputStream getOutputStream() {
	// TODO Auto-generated method stub
	return null;
}

@Override
public int waitFor() throws InterruptedException {
	// TODO Auto-generated method stub
	return 0;
}

}
