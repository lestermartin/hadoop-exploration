package lestermartin.hadoop.exploration.opengeorgia;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * See https://martin.atlassian.net/wiki/x/M4BmAQ for more details
 *
 *
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * 
 * The class definition requires four declarations: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (will be input key type for the reducer)
 *   The data type of the output value (the input value type for the reducer)
 */
public class TitleMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    private SalaryReportBuilder builder;

    @Override
    public void setup(Context context) {
        builder = new SalaryReportBuilder();
    }

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

        //turn the input CSV into an object
        SalaryReport salaryReport = builder.buildFromInputCsvFormat(value.toString());

        if( ! salaryReport.getOrgType().equals("LBOE") ) {
            return;  //not a school board, can ignore
        }

        if( salaryReport.getYear() != 2010 ) {
            return;  //not the year in question, can ignore
        }

        context.write(new Text(salaryReport.getTitle()), new FloatWritable(salaryReport.getSalary()));

	}

}
