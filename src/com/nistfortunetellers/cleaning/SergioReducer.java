package com.nistfortunetellers.cleaning;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class SergioReducer 
extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text keyText, Iterable<Text> lineTexts, Context context) 
			throws IOException, InterruptedException {
		String key = keyText.toString();
		
		ArrayList<ReducedMeasurement> outputMeasures = new ArrayList<ReducedMeasurement>();
		DescriptiveStatistics stats = new DescriptiveStatistics();
		
		//Obtain Date From Key
		String date = key.split(NISTClean.KEY_SEP)[1];

		//Iterate Through Keys and make array.
		for (Text lineText : lineTexts) {
			String line = lineText.toString();
			//check if we're dealing with a special key!
			if(line.contains(NISTClean.KEY_SPECIAL)) {
				//remove key chars
				line = line.substring(NISTClean.KEY_SPECIAL.length());
				outputMeasures.add(new ReducedMeasurement(date, line));
				continue;
			}
			ReducedMeasurement measure = new ReducedMeasurement(date, line);
			//add the flow values to our stats
			stats.addValue(measure.getFlow());
		}
		
		int median = (int)Math.round(stats.getPercentile(50));
		double stdDev = stats.getStandardDeviation();
		
		for(ReducedMeasurement outputMeasure : outputMeasures) {
			int flow = outputMeasure.getFlow();
			//if the flow is negative, or greater than one std devation away...
			if(flow < 0 || Math.abs(median - flow) > stdDev ) {
				//correct the value
				outputMeasure.correctFlow(median, NISTClean.SERGIO_REASON);
			}
			context.write(outputMeasure.submissionKey(), outputMeasure.submisionValue());
		}
	}
}

class ReducedMeasurement {
	private boolean flowCorrected = false;
	private String changedReason = "";
	private String date = "";
	private String laneID = "";
	
	private int flow;
	public ReducedMeasurement(String date, String line) {
		this.date = date;
		String[] splits = line.split(",");
		laneID = splits[0];
		flow = Integer.parseInt(splits[2]);
	}
	
	public int getFlow() {
		return flow;
	}
	
	public void correctFlow(int newFlow, String reason) {
		flowCorrected = true;
		changedReason = reason;
		flow = newFlow;
	}
	
	public Text submisionValue() {
		int changedVal;
		if(flowCorrected) {
			changedVal = 0;
		} else {
			changedVal = 1;
		}
		return new Text(changedVal + "\t" + flow + "\t" + changedReason);
	}
	
	public Text submissionKey() {
		return new Text(laneID + "\t" + date);
	}
}