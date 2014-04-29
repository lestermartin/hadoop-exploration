package lestermartin.hadoop.exploration.opengeorgia;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


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
public class SalaryStatisticsReducer extends Reducer<Text, FloatWritable, Text, Text> {

	/**
	 * The reduce method runs once for each key received from
	 * the shuffle and sort phase of the MapReduce framework
	 * (i.e. only one reducer will get mapper output values for
	 * a given key).
	 * 
	 * The method receives a key of type Text (in this case it will
	 * be a single job title), a set of values of type FloatWritable
	 * (1 or more "salary amounts"), and a reducer "Context" object.
	 */
	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {

        //context.write(key, new Text(values.iterator().))

		int numberOfPeopleWithThisJobTitle = 0;
        double totalSalaryAmount = 0.0d;
        double minSalary = Double.MAX_VALUE;
        double maxSalary = Double.MIN_VALUE;
			
		/**
		 * For each value in the set of values passed to us by the mapper:
		 */
		for(FloatWritable value : values) {
            float salary = value.get();
            numberOfPeopleWithThisJobTitle++;
            totalSalaryAmount = totalSalaryAmount + salary;

            if(salary < minSalary)
                minSalary = salary;
            if(salary > maxSalary)
                maxSalary = salary;
		}

        StringBuffer sb = new StringBuffer("{");
        sb.append(numberOfPeopleWithThisJobTitle);
        sb.append(",");
        sb.append(minSalary);
        sb.append(",");
        sb.append(maxSalary);
        sb.append(",");
        sb.append( totalSalaryAmount / numberOfPeopleWithThisJobTitle );
        sb.append("}");

		/**
		 * Call the write method on the Context object to emit a key
		 * and a value from the reduce method. 
		 */
		context.write(key, new Text(sb.toString()));
	}

}
