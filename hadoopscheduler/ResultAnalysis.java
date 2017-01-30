/**
 * 
 */
package hadoopscheduler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

/**
 * @author Daolin Cheng
 *
 */
public class ResultAnalysis {

	public static void getStat(String in, String out) {
		BufferedReader br = null;
		int jobCount = 0;
		int missed = 0;

		Map<String, Integer> jobs = new HashMap<String, Integer>();
		Map<String, Integer> jobTime = new HashMap<String, Integer>();

		try {
			br = new BufferedReader(new FileReader(in));
			String line = null;

			// Assume default encoding.
			FileWriter fileWriter = new FileWriter(out);

			// Always wrap FileWriter in BufferedWriter.
			BufferedWriter bw = new BufferedWriter(fileWriter);

			// skip first line, table head
			line = br.readLine();
			line = br.readLine();

			while ((line = br.readLine()) != null) {
				jobCount++;
				line = line.trim();

				// split running result
				StringTokenizer st = new StringTokenizer(line, "\t");
				// skip first token, job index
				st.nextToken();
				String job = st.nextToken();
				// skip third token, client index
				st.nextToken();
				int runningTime = Integer.valueOf(st.nextToken());
				if (!jobs.containsKey(job)) {
					jobs.put(job, 0);
					jobTime.put(job, 0);
				}
				
				jobs.put(job, jobs.get(job)+1);
				jobTime.put(job, jobTime.get(job)+ runningTime);

				if (st.hasMoreTokens()) {
					missed++;
				}
				line = br.readLine();
			}
			
			double missedRate = missed * 1.0 / jobCount;
			
			

			bw.write("Missed Rate = " + missedRate + "\r\n");
			bw.write("Count and average waitinging time of each job by folder:\r\n");
			for(Entry<String, Integer> e : jobs.entrySet()){
				bw.write(e.getKey() + "\t count = " + e.getValue()+ "\t avg = " + jobTime.get(e.getKey()) / e.getValue() +"\r\n");
			}
			
			br.close();
			bw.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**EDF 6800 3000 12800 data.txt
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String input="EDF 8800 3000 15800 data.txt";
		String output="EDF 8800 3000 15800 data_stat.txt";
		getStat(input,output);/*InputFile, OutputFile*/

	}

}
