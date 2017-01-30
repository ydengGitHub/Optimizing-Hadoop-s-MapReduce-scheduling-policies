package hadoopscheduler;

import java.util.Comparator;
import java.util.PriorityQueue;


public class FIFO extends Scheduler {

	/**
	 * 
	 */
	public FIFO() {
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
				return o1.arriveTime - o2.arriveTime;
			}			
		});

	}

}
