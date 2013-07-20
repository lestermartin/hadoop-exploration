package lestermartin.hadoop.exploration.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/** 
 * To define a reduce function for your MapReduce job, subclass 
 * the Reducer class and override the reduce method.
 * 
 * The class definition requires four declarations: 
 *   The data type of the input key (the mapper's output key type) 
 *   The data type of the input value (mapper's output value type)
 *   The data type of the output key
 *   The data type of the output value
 */   
public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	/**
	 * The reduce method runs once for each key received from
	 * the shuffle and sort phase of the MapReduce framework
	 * (i.e. only one reducer will get mapper output values for
	 * a given key).
	 * 
	 * The method receives a key of type Text (in this case it will
	 * be a single word), a set of values of type IntWritable
	 * (1 or more "counts"), and a reducer "Context" object.
	 */
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

		int wordCount = 0;
			
		/**
		 * For each value in the set of values passed to us by the mapper:
		 */
		for(IntWritable value : values) {
			/**
			 * Add the value to the word count counter for this key.
			 */
			wordCount += value.get();
		}
			
		/**
		 * Call the write method on the Context object to emit a key
		 * and a value from the reduce method. 
		 */
		context.write(key, new IntWritable(wordCount));
	}

}
