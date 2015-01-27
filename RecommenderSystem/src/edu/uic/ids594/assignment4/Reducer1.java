package edu.uic.ids594.assignment4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reducer1 extends MapReduceBase
implements Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterator<Text> value,
			OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub

		ArrayList<String> store = new ArrayList<String>();
		String data = "";

		while(value.hasNext()){
			store.add(value.next().toString());
		}
		//sort the arraylist
		Collections.sort(store);

		//
		for(int i=0;i<store.size();i++){
			data=data+store.get(i)+"\t";
		}
		// collect the output in <Text, Text> format
		collector.collect(key,new Text(data));

		//System.out.println("Key: " + key + " Sorted Values: " + data);


	}


}
