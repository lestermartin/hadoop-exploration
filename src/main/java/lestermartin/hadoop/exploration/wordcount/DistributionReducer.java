package lestermartin.hadoop.exploration.wordcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/** 
 * This mapper is a spin on the SumReducer one.  For each word we now
 * want a count for each individual input file (which could be only one). 
 */   
public class DistributionReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> fileNames, Context context)
				throws IOException, InterruptedException {

		Map<String,Integer> fileNameCounts = new HashMap<String,Integer>(); 
			
		for (Text fileNameAsText : fileNames) {
			String fileName = fileNameAsText.toString();  //get the file name
			if( fileNameCounts.containsKey(fileName) ) {  //have we already seen it?
				//determine new value
				int updatedCounterValue = ((Integer) fileNameCounts.get(fileName)).intValue() + 1;
				//and update it in the map
				fileNameCounts.put(fileName, new Integer(updatedCounterValue));
			} else { 
				//stand up an initial counter if we haven't see the name before
				fileNameCounts.put(fileName, new Integer(1));
			}
		}
		
		//build an output CSV string with each value being the file name
		//  and the count of the word in just that file name
		StringBuffer distribution = new StringBuffer();
		boolean pastFirst = false;
		for(Entry<String, Integer> oneEntry : fileNameCounts.entrySet()) {
			if( pastFirst ) {
				distribution.append(",");
			}
			pastFirst = true;
			distribution.append(oneEntry.getKey());
			distribution.append("=");
			distribution.append(oneEntry.getValue().toString());
		}
			
		context.write(key, new Text(distribution.toString()));
	}

}
