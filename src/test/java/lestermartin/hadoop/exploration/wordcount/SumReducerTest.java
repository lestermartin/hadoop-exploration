package lestermartin.hadoop.exploration.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;


public class SumReducerTest {

	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	
	@Before
	public void setUp() {
		SumReducer reducer = new SumReducer();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		reduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testReducerWithOneWordWithOneCount() throws IOException {
		List<IntWritable> catValues = new ArrayList<IntWritable>();
		catValues.add(new IntWritable(1));
		reduceDriver.withInput(new Text("cat"), catValues); 
		reduceDriver.withOutput(new Text("cat"), new IntWritable(1));
		reduceDriver.runTest();
	}

	@Test
	public void testReducerWithTwoWordsWithOneCountEach() throws IOException {
		List<IntWritable> catValues = new ArrayList<IntWritable>();
		catValues.add(new IntWritable(1));
		reduceDriver.withInput(new Text("cat"), catValues); 
		List<IntWritable> dogValues = new ArrayList<IntWritable>();
		dogValues.add(new IntWritable(1));
		reduceDriver.withInput(new Text("dog"), dogValues); 
		reduceDriver.withOutput(new Text("cat"), new IntWritable(1));
		reduceDriver.withOutput(new Text("dog"), new IntWritable(1));
		reduceDriver.runTest();
	}

	@Test
	public void testReducerWithFullSentence() throws IOException {
		//test string "Now is the time for all good men to come to the aid of their country."
		
		List<IntWritable> nowValues = new ArrayList<IntWritable>();
		nowValues.add(new IntWritable(1));
		reduceDriver.withInput(new Text("Now"), nowValues); 
		
		List<IntWritable> isValues = new ArrayList<IntWritable>();
		isValues.add(new IntWritable(1));
		reduceDriver.withInput(new Text("is"), isValues); 

		//THERE ARE TWO 'the' values
		List<IntWritable> theValues = new ArrayList<IntWritable>();
		theValues.add(new IntWritable(1));  //FIRST ONE
		theValues.add(new IntWritable(1));  //SECOND ONE
		reduceDriver.withInput(new Text("the"), theValues); 
		
		List<IntWritable> timeValues = new ArrayList<IntWritable>();
		timeValues.add(new IntWritable(1));
		reduceDriver.withInput(new Text("time"), timeValues); 
		
		List<IntWritable> forValues = new ArrayList<IntWritable>();
		forValues.add(new IntWritable(1));
		reduceDriver.withInput(new Text("for"), forValues); 
		
		List<IntWritable> allValues = new ArrayList<IntWritable>();
		allValues.add(new IntWritable(1));
		reduceDriver.withInput(new Text("all"), allValues); 
		
		List<IntWritable> goodValues = new ArrayList<IntWritable>();
		goodValues.add(new IntWritable(1));
		reduceDriver.withInput(new Text("good"), goodValues); 
		
		List<IntWritable> menValues = new ArrayList<IntWritable>();
		menValues.add(new IntWritable(1));
		reduceDriver.withInput(new Text("men"), menValues); 
		
		//THERE ARE TWO 'to' values
		List<IntWritable> toValues = new ArrayList<IntWritable>();
		toValues.add(new IntWritable(1));  //FIRST ONE
		toValues.add(new IntWritable(1));  //SECOND ONE
		reduceDriver.withInput(new Text("to"), toValues); 
		
		List<IntWritable> comeValues = new ArrayList<IntWritable>();
		comeValues.add(new IntWritable(1));
		reduceDriver.withInput(new Text("come"), comeValues); 
		
		List<IntWritable> aidValues = new ArrayList<IntWritable>();
		aidValues.add(new IntWritable(1));
		reduceDriver.withInput(new Text("aid"), aidValues); 
		
		List<IntWritable> ofValues = new ArrayList<IntWritable>();
		ofValues.add(new IntWritable(1));
		reduceDriver.withInput(new Text("of"), ofValues); 
		
		List<IntWritable> theirValues = new ArrayList<IntWritable>();
		theirValues.add(new IntWritable(1));
		reduceDriver.withInput(new Text("their"), theirValues); 
		
		List<IntWritable> countryValues = new ArrayList<IntWritable>();
		countryValues.add(new IntWritable(1));
		reduceDriver.withInput(new Text("country"), countryValues); 
		
		reduceDriver.withOutput(new Text("Now"), new IntWritable(1));
		reduceDriver.withOutput(new Text("is"), new IntWritable(1));
		reduceDriver.withOutput(new Text("the"), new IntWritable(2));	//TWO 'the' values
		reduceDriver.withOutput(new Text("time"), new IntWritable(1));
		reduceDriver.withOutput(new Text("for"), new IntWritable(1));
		reduceDriver.withOutput(new Text("all"), new IntWritable(1));
		reduceDriver.withOutput(new Text("good"), new IntWritable(1));
		reduceDriver.withOutput(new Text("men"), new IntWritable(1));
		reduceDriver.withOutput(new Text("to"), new IntWritable(2));	//TWO 'to' values
		reduceDriver.withOutput(new Text("come"), new IntWritable(1));
		reduceDriver.withOutput(new Text("aid"), new IntWritable(1));
		reduceDriver.withOutput(new Text("of"), new IntWritable(1));
		reduceDriver.withOutput(new Text("their"), new IntWritable(1));
		reduceDriver.withOutput(new Text("country"), new IntWritable(1));

		reduceDriver.runTest();
	}

}
