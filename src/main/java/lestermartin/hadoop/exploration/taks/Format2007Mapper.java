package lestermartin.hadoop.exploration.taks;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;



public class Format2007Mapper extends Mapper<LongWritable, Text, Text, Text> {

    static String token = ",";

	@Override
	public void map(LongWritable key, Text value, Context context)
					throws IOException, InterruptedException {

	    /*
	     * Convert the line, which is received as a Text object,
	     * to a String object.
	     */
		String line = value.toString();

        StringBuilder studentValues = new StringBuilder();
        List<String> allTokens = new ArrayList<String>();
        StringTokenizer st =  new StringTokenizer(line, token);
        while(st.hasMoreTokens()) {
            allTokens.add(st.nextToken());
        }

        studentValues.append(allTokens.get(1));  //test year
        studentValues.append(token);
        studentValues.append(allTokens.get(3));  //test month
        studentValues.append(token);
        studentValues.append(allTokens.get(2));  //grade level
        studentValues.append(token);
        studentValues.append(allTokens.get(4));  //campus
        studentValues.append(token);
        studentValues.append(allTokens.get(12));  //gender
        studentValues.append(token);
        studentValues.append(allTokens.get(9));  //ethnicity

        context.write(new Text(allTokens.get(0)), new Text(studentValues.toString()));
	}

}
