package lestermartin.hadoop.exploration.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;


public class WordCountTest {
	
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		WordMapper mapper = new WordMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		SumReducer reducer = new SumReducer();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapReduceWithTwoWords() throws IOException {
		mapReduceDriver.withInput(new LongWritable(1), new Text("cat dog"));
		mapReduceDriver.withOutput(new Text("cat"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("dog"), new IntWritable(1));
		mapReduceDriver.runTest();
	}

	@Test
	public void testMapReduceWithFullSentenceTwice() throws IOException {
		mapReduceDriver.withInput(new LongWritable(1), 
				new Text("Now is the time for all good men to come to the aid of their country."));
		mapReduceDriver.withInput(new LongWritable(1), 
				new Text("Now is the time for all good men to come to the aid of their country."));
		//have to put them in SORTED order!!
		mapReduceDriver.withOutput(new Text("Now"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("aid"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("all"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("come"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("country"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("for"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("good"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("is"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("men"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("of"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("the"), new IntWritable(4));	//FOUR 'the' values
		mapReduceDriver.withOutput(new Text("their"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("time"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("to"), new IntWritable(4));		//FOUR 'to' values
		mapReduceDriver.runTest();
	}

	@Test
	public void testMapReduceWithFullSentenceTwiceAndTwoWordsInbetweenFiveTimes() throws IOException {
		mapReduceDriver.withInput(new LongWritable(1), 
				new Text("Now is the time for all good men to come to the aid of their country."));
		mapReduceDriver.withInput(new LongWritable(1), new Text("cat dog"));
		mapReduceDriver.withInput(new LongWritable(1), new Text("cat dog"));
		mapReduceDriver.withInput(new LongWritable(1), new Text("cat dog"));
		mapReduceDriver.withInput(new LongWritable(1), new Text("cat dog"));
		mapReduceDriver.withInput(new LongWritable(1), new Text("cat dog"));
		mapReduceDriver.withInput(new LongWritable(1), 
				new Text("Now is the time for all good men to come to the aid of their country."));
		//have to put them in SORTED order!!
		mapReduceDriver.withOutput(new Text("Now"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("aid"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("all"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("cat"), new IntWritable(5)); 	//FIVE 'cat' values
		mapReduceDriver.withOutput(new Text("come"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("country"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("dog"), new IntWritable(5)); 	//FIVE 'dog' values
		mapReduceDriver.withOutput(new Text("for"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("good"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("is"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("men"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("of"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("the"), new IntWritable(4));	//FOUR 'the' values
		mapReduceDriver.withOutput(new Text("their"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("time"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("to"), new IntWritable(4));		//FOUR 'to' values
		mapReduceDriver.runTest();
	}

}
