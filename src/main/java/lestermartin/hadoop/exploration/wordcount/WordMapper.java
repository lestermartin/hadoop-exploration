package lestermartin.hadoop.exploration.wordcount;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * 
 * The class definition requires four declarations: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (will be input key type for the reducer)
 *   The data type of the output value (the input value type for the reducer)
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	/**
	 * The map method runs once for each line of text in the input file.
	 * The method receives a key of type LongWritable (which will be the
	 * byte offset of the file), a value of type Text (contains the actual
	 * string representation of a single row in the input file), and a 
	 * mapper "Context" object used to interact with the MR framework.
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {
	    /*
	     * Convert the line, which is received as a Text object,
	     * to a String object.
	     */
		String line = value.toString();
		
		List<String> words = WordUtils.splitWords(line);
		
		for (String word : words) {
			/*
			 * Call the write method on the Context object to emit 
			 * a key and a value from the map method.
			 */
			context.write(new Text(word), new IntWritable(1));
		}		
	}

}
