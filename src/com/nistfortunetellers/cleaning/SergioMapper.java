package com.nistfortunetellers.cleaning;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;

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
		//retrieve all of the relevant keys.
		String[] keys = measure.getKeys();
		//write them to the mapper.
		for(String key : keys) {
			context.write(new Text(key), lineText);
		}
	}
}




