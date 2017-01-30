package hadoopscheduler;

import java.util.PriorityQueue;

public abstract class Scheduler {
	PriorityQueue<WordCountJob> queue;
	
	public abstract void initQueue();
	
	public boolean isEmpty(){
		return queue.isEmpty();
	}
	
	public void add(WordCountJob job){
		queue.offer(job);
	}
	
	public WordCountJob getJob(){
		return queue.poll();
	}
	
}
