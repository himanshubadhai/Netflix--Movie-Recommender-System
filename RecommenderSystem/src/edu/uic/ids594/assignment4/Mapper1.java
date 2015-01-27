package edu.uic.ids594.assignment4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Mapper1 extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text>{

	// initialize variables
	private Text customerid = new Text();
	private Text movieid_rating = new Text();
	//private Text rating = new Text();
	//private Text timestamp = new Text();

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {

		// read line by line and split them by tab
		String lineReader = value.toString();
		String[] tokenizer = lineReader.split("\\t");

		//setting output key
		this.customerid.set(tokenizer[0]);

		//setting output value
		this.movieid_rating.set(tokenizer[1] + ","+ tokenizer[2]);

		//collecting output variables
		collector.collect(this.customerid, this.movieid_rating);
		//System.out.println(this.customerid + "  " + this.movieid_rating);

	}
}
