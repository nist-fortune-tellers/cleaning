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
	
	public void map(Object unneeded, Text txt, Context context) 
			throws IOException, InterruptedException {
		
		//check for special case of being top line in file
		
		//if not, parse the line into an object!
		Measurement measure = new Measurement(context.getConfiguration(), txt);
		
		
		
		String key = "";
		String value = "";
		context.write(new Text(key), new Text(value));  
	}
}

class Measurement {
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	
	private String laneID;
	private int zoneID;
	private Calendar calendar;
	
	//Creates a Measurment object based off of a line.
	public Measurement(Configuration config, Text lineText) {
		String line = lineText.toString();
		if (line.contains("lane_id,measurement_start,speed,flow,occupancy,quality")) {
			//go ahead and exit, since this line doesn't matter.
			throw new IllegalArgumentException("Is CSV Header Def Line");
		}
		
		String[] splits = line.split(",");
		//do a sanity check on the line. Make sure all the elements exist.
		if (splits.length != 6) {
			return;
		}
		
		/* Retrieve Needed Values */
		laneID = splits[0];
		zoneID = config.getInt(laneID, -1);
		//if the zone ID wasn't found, return.
		if(zoneID == -1) {
			throw new IllegalArgumentException("Zone ID not found.");
		}
		//let's now extract the date
		//ex. 2006-09-01 00:00:07-04. 
		//The below code cuts off the above example, to be an easily parsable string
		// like 2006-09-01 00:00
		String dateStr = splits[1].substring(0, 16);
		Date date;
		try {
			date = sdf.parse(dateStr);
		} catch (ParseException e) {
			throw new IllegalArgumentException("Unable to Parse Date.");
		}
		calendar = GregorianCalendar.getInstance();
		calendar.setTime(date);
	}
}


