package lestermartin.hadoop.exploration.wordcount;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


/**
 * This mapper is a spin on the WordMapper one.  It still creates a key
 * of the "word", but the value is the source (i.e. the file name) of 
 * where the word was found.  This makes sense for when you are using 
 * multiple input files as input.
 */
public class WordSourceMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
		
		//get the file where the word came from
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

		List<String> words = WordUtils.splitWords(value.toString());
		
		for (String word : words) {
			//associate the word to the filename
			context.write(new Text(word), new Text(fileName));
		}
	}	
	
}
