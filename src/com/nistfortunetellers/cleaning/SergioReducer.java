package com.nistfortunetellers.cleaning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SergioReducer 
extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text keyText, Iterable<Text> lineTexts, Context context) 
			throws IOException, InterruptedException {
		String key = keyText.toString();
		
		ArrayList<FlowSortableMeasurement> measures = new ArrayList<FlowSortableMeasurement>();

		//Iterate Through Keys and make array.
		for (Text lineText : lineTexts) {
			String line = lineText.toString();
			measures.add(new FlowSortableMeasurement(context.getConfiguration(), line));
		}
		//sort the arraylist!
		Collections.sort(measures);
	}
}

class FlowSortableMeasurement extends Measurement implements Comparable<FlowSortableMeasurement> {

	public FlowSortableMeasurement(Configuration config, String line) {
		super(config, line);
	}

	@Override
	public int compareTo(FlowSortableMeasurement o) {
		return new Integer(getFlow()).compareTo(o.getFlow());
	}

}