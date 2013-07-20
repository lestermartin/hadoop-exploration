package lestermartin.hadoop.exploration.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/** 
 * Map-Only job whose output values are expressed as job-level counters
 * that can be found in the Job Tracker UI
 */
public class SizeCounter {
	
	public static void main(String[] args) throws Exception {
		
		if(args.length != 2) {
			System.out.println("Usage: WordCount <input dir> <output dir>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(SizeCounter.class);
		job.setJobName("Word Size Counter");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//even though we don't have any real developer-generated output, we
		//  still need the output directory for Hadoop to put his information
		//  about how the job ran
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//map-only job, so we do not call setReducerClass
		job.setMapperClass(WordSizeMapper.class);
		//and we set the number of reduce tasks to 0
		job.setNumReduceTasks(0);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);

	}

}
