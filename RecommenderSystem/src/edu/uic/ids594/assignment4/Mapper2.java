package edu.uic.ids594.assignment4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Mapper2 extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {

		// Declare variables
		Text moviePair = new Text();
		Text ratingPair = new Text();

		ArrayList<Integer> movieArray = new ArrayList<Integer>();
		ArrayList<Integer> ratingArray = new ArrayList<Integer>();

		// read line and split it using tab
		String lineReader = value.toString();
		String[] tokenizer = lineReader.split("\\t");

		// populate the movie and rating arrays
		for (int i= 1; i < tokenizer.length; i++)
		{
			String[] temp = tokenizer[i].split(",");
			movieArray.add(Integer.parseInt(temp[0]));
			ratingArray.add(Integer.parseInt(temp[1]));
		}
		// sort the arrays
		Collections.sort(movieArray);
		Collections.sort(ratingArray);

		// construct the movie and rating pairs
		for (int i=0; i < tokenizer.length-1; i++)
		{
			for (int j=i+1; j < tokenizer.length-1; j++)
			{
				moviePair.set(movieArray.get(i).toString() + ","+ movieArray.get(j).toString());
				ratingPair.set(ratingArray.get(i).toString()+","+ratingArray.get(j).toString());
				collector.collect(moviePair, ratingPair);
				//System.out.println(movieArray.get(i) + ","+ movieArray.get(j));
				//System.out.println(ratingArray.get(i)+","+ratingArray.get(j));
			}
		}
	}

}