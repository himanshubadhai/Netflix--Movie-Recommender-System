package edu.uic.ids594.assignment4;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Scanner;

public class Prediction {

	// create variables to store the file paths
	private static final String filePath = "/Users/himansubadhai/Documents/input/assignment4/cf/u.data";
	private static final String reducerOutput = "/Users/himansubadhai/Documents/input/assignment4/output/sample.txt";


	// 2-D matrix to store vales in the format matrix[userid][movieid]= rating
	private int[][] moviematrix= new int[943][1682];

	//initialize variables
	private BufferedReader reader;
	private HashMap<String,Double> hashmap = new HashMap<String,Double>();
	ArrayList<String> valueArray = new ArrayList<String>();

	private HashMap<Integer,ArrayList<String>> predictedMap = new HashMap<Integer,ArrayList<String>>();

	String key="";
	double similarity=0.0;
	int rating=0;

	// method to read input file and construct the matrix
	public void readFile (){

		try {
			reader=new BufferedReader(new FileReader(filePath));

			//read line by line
			String line=reader.readLine().trim(); 

			while(line!=null){		
				String lineReader = line.toString();
				String[] tokenizer = lineReader.split("\\t");

				int userid = Integer.parseInt(tokenizer[0]);
				int movieid = Integer.parseInt(tokenizer[1]);
				int rating = Integer.parseInt(tokenizer[2]);

				moviematrix[userid-1][movieid-1] = rating;
				line = reader.readLine(); 
			}
			System.out.println("matrix created successfully");
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch(IOException ex){
			ex.printStackTrace();
		}
	}

	// method to read the reducer output and populate a hashmap
	public void populateMap(){
		try {
			//System.out.println("pmaps");
			reader=new BufferedReader(new FileReader(reducerOutput));

			//read line by line
			String line=reader.readLine().trim(); 

			while(line!=null){		
				String lineReader = line.toString();
				String[] tokenizer = lineReader.split("\\t");
				String key = tokenizer[0];
				double value = Double.parseDouble(tokenizer[1]);
				//System.out.println("hash key value: "+ key+ "  "+value);
				hashmap.put(key, value);
				line=reader.readLine();
			}
			System.out.println("hashmap populated successfully");
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch(IOException ex){
			ex.printStackTrace();
		}
	}
	public void predictmovies (int userid, int topwords){

		for(int i=0; i < 1682; i++){
			if(moviematrix[userid-1][i] == 0.0){
				calculatePrediction(userid,i);
			}
		}
		ArrayList<String> predictionsList = predictedMap.get(userid);
		Collections.sort(predictionsList,Collections.reverseOrder());

		System.out.println("Your top "+topwords+" recommended movies are: ");

		for (int i=0; i< topwords;i++){
			String predictionsTemp = predictionsList.get(i);
			String[] tokenizer = predictionsTemp.split(",");
			System.out.println(tokenizer[1]);
		}
	}

	//this method calculates the prediction
	public void calculatePrediction (int userid, int item){

		double numerator=0.0;
		double denominator=0.0;

		double predictedValues=0.0;
		String value;


		for(int i=0; i < 1682; i++){

			// construct key based on the condition
			//set similarity to 0.0 if movie is not rated and not available in reducer output
			if(i+1>item){
				key = item+","+(i+1);
				if(hashmap.containsKey(key)){
					similarity =hashmap.get(key);
				}
				else{
					similarity=0.0;
				}
			}
			else{
				key = (i+1)+","+item;
				if(hashmap.containsKey(key)){
					similarity =hashmap.get(key);
				}
				else{
					similarity=0.0;
				}
			}

			rating = moviematrix[userid-1][i];
			if(rating!=0)
			{
				numerator = numerator + rating*similarity;
				denominator = denominator + Math.abs(similarity);
			}
		}
		//calculate prediction
		if(denominator!=0){
			predictedValues = numerator/denominator;
		}
		value = predictedValues +","+item;
		//System.out.println("value: "+ value);
		valueArray.add(value);

		//this hashmap contains all the movie predictions for a gievn user
		predictedMap.put(userid,valueArray);
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Prediction prediction = new Prediction();
		prediction.readFile();
		prediction.populateMap();

		Scanner in = new Scanner(System.in);

		System.out.println("Enter userid: ");
		int userid = Integer.parseInt(in.nextLine());

		System.out.println("Enter no. of top movies: ");
		int topmovies = Integer.parseInt(in.nextLine());

		prediction.predictmovies(userid, topmovies);
		in.close();
	}

}
