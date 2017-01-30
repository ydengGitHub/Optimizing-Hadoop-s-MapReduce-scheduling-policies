package hadoopscheduler;

import java.util.Comparator;
import java.util.PriorityQueue;


public class SRT extends Scheduler {

	/**
	 * 
	 */
	public SRT() {
		super();
		initQueue();
	}
	
	@Override
	public void initQueue() {
		this.queue = new PriorityQueue<WordCountJob>(10, new Comparator<WordCountJob>() {

			@Override
			/**
			 * returns shortest running time on priorityqueue
			 */
			public int compare(WordCountJob o1, WordCountJob o2) {
				return o1.runningTime - o2.runningTime;
			}			
		});

	}

}
