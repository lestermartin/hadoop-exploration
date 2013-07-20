package lestermartin.hadoop.exploration.wordcount;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordSizeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	/**
	 * This mapper simple updates a custom job counter for each word size.
	 * It outputs nothing and is intended to be run as a map-only job that
	 * will (via the Job Tracker UI) tell the operator how many words 
	 * there are for each size.
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
		
		List<String> words = WordUtils.splitWords(value.toString());
		
		for(String word : words) {
			context.getCounter("WordsByLength", String.valueOf(word.length())).increment(1);
		}		
	}	
	
}
