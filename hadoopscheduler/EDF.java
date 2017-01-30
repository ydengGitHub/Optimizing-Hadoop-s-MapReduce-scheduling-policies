package hadoopscheduler;

import java.util.Comparator;
import java.util.PriorityQueue;


public class EDF extends Scheduler {
	
	

	/**
	 * 
	 */
	public EDF() {
		super();
		initQueue();
	}

	@Override
	public void initQueue() {
		this.queue = new PriorityQueue<WordCountJob>(10, new Comparator<WordCountJob>() {

			@Override
			public int compare(WordCountJob o1, WordCountJob o2) {
				return o1.deadline - o2.deadline;
			}			
		});

	}

}
