package com.nistfortunetellers.cleaning;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SergioMapper 
extends Mapper<Object, Text, Text, Text>{
	
	public void map(Object unneeded, Text lineText, Context context) 
			throws IOException, InterruptedException {
		String line = lineText.toString();
		//check for special case of being top line in file
		if (line.contains("lane_id,measurement_start,speed,flow,occupancy,quality")) {
			//go ahead and exit, since this line doesn't matter.
			return;
		}
		//if not, parse the line into an object!
		Measurement measure = new Measurement(context.getConfiguration(), line);
		//write the current item in a special way
		context.write(measure.selfMapKey(), new Text(NISTClean.KEY_SPECIAL + line));
		//retrieve all of the relevant keys.
		String[] keys = measure.timeKeys();
		//write them to the mapper.
		for(String key : keys) {
			context.write(new Text(key), lineText);
		}
	}
}




