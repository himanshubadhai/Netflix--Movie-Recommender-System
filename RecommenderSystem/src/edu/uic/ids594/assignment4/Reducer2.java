package edu.uic.ids594.assignment4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reducer2 extends MapReduceBase
implements Reducer<Text, Text, Text, DoubleWritable>{
	
	//private HashMap<String,Double> hashmap = new HashMap<String,Double>();

	@Override
	public void reduce(Text key, Iterator<Text> value,
			OutputCollector<Text, DoubleWritable> collector, Reporter reporter)
					throws IOException {
		// TODO Auto-generated method stub

		// declare 2 ArrayList to store ratings 
		ArrayList<Integer> ratingsArray1 = new ArrayList<Integer>();
		ArrayList<Integer> ratingsArray2 = new ArrayList<Integer>();

		// declare variables
		double dotproduct=0;
		int squareArray1=0;
		int squareArray2=0;
		double similarity=0;

		//populate the rating arrays
		while(value.hasNext()){
			String[] tokenizer = value.next().toString().split(",");
			ratingsArray1.add(Integer.parseInt(tokenizer[0]));
			ratingsArray2.add(Integer.parseInt(tokenizer[1]));
		}	

		// calculate the dot product and mod values
		for(int i=0; i < Math.max(ratingsArray1.size(),ratingsArray2.size()); i++){

			dotproduct =dotproduct + (ratingsArray1.get(i)*ratingsArray2.get(i));
			squareArray1 = squareArray1 + ratingsArray1.get(i)*ratingsArray1.get(i);
			squareArray2 = squareArray2 + ratingsArray2.get(i)*ratingsArray2.get(i);
		}

		// calculate similarity
		similarity = dotproduct/(Math.sqrt(squareArray1)*Math.sqrt(squareArray2));
		
		//similarity = Math.round(similarity * 100.0)/100.0;

		collector.collect(key,new DoubleWritable(similarity));
		//System.out.println("key: " + key + "  value: "+ similarity);
	}

}