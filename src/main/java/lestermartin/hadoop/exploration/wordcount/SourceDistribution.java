package lestermartin.hadoop.exploration.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class SourceDistribution {
	
	public static void main(String[] args) throws Exception {
		
		if(args.length != 2) {
			System.out.println("Usage: SourceDistribution <input dir> <output dir>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(SourceDistribution.class);
		job.setJobName("Word Source Distribution");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(WordSourceMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(DistributionReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);

	}

}
