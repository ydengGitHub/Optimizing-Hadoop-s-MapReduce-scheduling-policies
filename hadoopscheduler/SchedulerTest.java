package hadoopscheduler;

public class SchedulerTest {

	public static void main(String[] args) {
		WordCountJob a = new WordCountJob(1, 4, 5, "");
		WordCountJob b = new WordCountJob(2, 2, 7, "");
		WordCountJob	c = new WordCountJob(3, 3, 6, "");
		
		EDF edf = new EDF();
		SRT srt = new SRT();
		
		
		//--------  Earliest deadline ---------
		//ģ��ʱ������
		int jobEnd = 0;
		for (int i = 0; i < 100; i++) {
			if (i == 0){
				edf.add(a);
			}
			
			if (i == 1){
				edf.add(b);
			}
			
			if (i == 3){
				edf.add(c);
			}
			
			if(jobEnd == i && !edf.queue.isEmpty()){
				// get job to run until it finishes
				WordCountJob j = edf.queue.poll();
				jobEnd = i + j.runningTime;
				System.out.println(j.clientIndex + " is running;");
			}
			
		}
		
		//------  Shortest Running Time -----------
		jobEnd = 0;
		for (int i = 0; i < 100; i++) {
			if (i == 0){
				srt.add(a);
			}
			
			if (i == 1){
				srt.add(b);
			}
			
			if (i == 3){
				srt.add(c);
			}
			
			if(jobEnd == i && !srt.queue.isEmpty()){
				// get job to run until it finishes
				WordCountJob j = srt.queue.poll();
				jobEnd = i + j.runningTime;
				System.out.println(j.clientIndex + " is running;");
			}
			
		}

	}

}
