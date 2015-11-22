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
		//String key = keyText.toString();
		
		ArrayList<Measurement> outputMeasures = new ArrayList<Measurement>();
		DescriptiveStatistics stats = new DescriptiveStatistics();

		//Iterate Through Keys and make array.
		for (Text lineText : lineTexts) {
			String line = lineText.toString();
			//check if we're dealing with a special key!
			if(line.contains(NISTClean.KEY_SPECIAL)) {
				//remove key chars
				line = line.substring(NISTClean.KEY_SPECIAL.length());
				outputMeasures.add(new Measurement(context.getConfiguration(), line));
				continue;
			}
			Measurement measure = new Measurement(context.getConfiguration(), line);
			//add the flow values to our stats
			stats.addValue(measure.getFlow());
		}
		
		int median = (int)Math.round(stats.getPercentile(50));
		double stdDev = stats.getStandardDeviation();
		
		for(Measurement outputMeasure : outputMeasures) {
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
