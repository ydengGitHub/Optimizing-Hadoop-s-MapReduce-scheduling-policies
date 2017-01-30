package hadoopscheduler;

public class WordCountJob {

	public int clientIndex;
	public int arriveTime;
	public int startTime = -1; // set to be -1 before scheduler decides its schedule
	public int runningTime;
	public int deadline;
	public String inputFolder = null;
	
	

	/**
	 * @param arriveTime
	 * @param runningTime
	 * @param deadline
	 * @param dataFile
	 */
	public WordCountJob(int index, int periodTime, int runningTime, String dataFile) {
		super();
		this.clientIndex = index;
		this.arriveTime = (int) (System.currentTimeMillis()-Server.initialTime);;
		this.runningTime = runningTime;
		this.deadline = arriveTime+periodTime;
		this.inputFolder = dataFile;
	}
}
