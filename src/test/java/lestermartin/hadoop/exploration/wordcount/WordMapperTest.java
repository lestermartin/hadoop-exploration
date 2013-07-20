package lestermartin.hadoop.exploration.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;


public class WordMapperTest {
	
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	
	@Before
	public void setUp() {
		WordMapper mapper = new WordMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);
	}
	
	@Test
	public void testMapperWithTwoWords() throws IOException {
		mapDriver.withInput(new LongWritable(1), new Text("cat dog"));
		mapDriver.withOutput(new Text("cat"), new IntWritable(1));
		mapDriver.withOutput(new Text("dog"), new IntWritable(1));
		mapDriver.runTest();
	}

	@Test
	public void testMapperWithFullSentence() throws IOException {
		mapDriver.withInput(new LongWritable(1), 
				new Text("Now is the time for all good men to come to the aid of their country."));
		mapDriver.withOutput(new Text("Now"), new IntWritable(1));
		mapDriver.withOutput(new Text("is"), new IntWritable(1));
		mapDriver.withOutput(new Text("the"), new IntWritable(1));
		mapDriver.withOutput(new Text("time"), new IntWritable(1));
		mapDriver.withOutput(new Text("for"), new IntWritable(1));
		mapDriver.withOutput(new Text("all"), new IntWritable(1));
		mapDriver.withOutput(new Text("good"), new IntWritable(1));
		mapDriver.withOutput(new Text("men"), new IntWritable(1));
		mapDriver.withOutput(new Text("to"), new IntWritable(1));
		mapDriver.withOutput(new Text("come"), new IntWritable(1));
		mapDriver.withOutput(new Text("to"), new IntWritable(1));
		mapDriver.withOutput(new Text("the"), new IntWritable(1));
		mapDriver.withOutput(new Text("aid"), new IntWritable(1));
		mapDriver.withOutput(new Text("of"), new IntWritable(1));
		mapDriver.withOutput(new Text("their"), new IntWritable(1));
		mapDriver.withOutput(new Text("country"), new IntWritable(1));
		mapDriver.runTest();
	}

}
