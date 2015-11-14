package com.nistfortunetellers.cleaning;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.conf.Configuration;

class Measurement {
	
	private SimpleDateFormat df = new SimpleDateFormat(NISTClean.DATE_FORMAT);
	private String laneID;
	private int zoneID;
	private int flow;
	private Calendar calendar;
	
	private boolean flowChanged = false;
	private String changedReason = "";
	
	//Creates a Measurment object based off of a line.
	public Measurement(Configuration config, String line) {
		
		String[] splits = line.split(",");
		//do a sanity check on the line. Make sure all the elements exist.
		if (splits.length != 6) {
			return;
		}
		
		/* Retrieve Needed Values */
		/* Lane ID */
		laneID = splits[0];
		zoneID = config.getInt(laneID, -1);
		//if the zone ID wasn't found, return.
		if(zoneID == -1) {
			throw new IllegalArgumentException("Zone ID not found.");
		}
		/* Date */
		// ex. 2006-09-01 00:00:07-04. 
		//The below code cuts off the above example, to be an easily parsable string
		//like 2006-09-01 00:00
		String dateStr = splits[1].substring(0, 16);
		Date date;
		try {
			date = df.parse(dateStr);
		} catch (ParseException e) {
			throw new IllegalArgumentException("Unable to Parse Date.");
		}
		calendar = GregorianCalendar.getInstance();
		calendar.setTime(date);
		/* Flow */
		String flowStr = splits[3];
		flow = Integer.parseInt(flowStr);
	}
	
	private static final int NUM_KEYS = 11;
	
	//Returns all of the needed calendar ranges
	private Calendar[] getTimeRange() {
		//Original Calendar Time
		Calendar[] times = new Calendar[NUM_KEYS];
		//add the unmodified time
		times[0] = (Calendar) calendar.clone();
		for(int i = 1; i != NUM_KEYS; ++i) {
			Calendar cal = (Calendar) calendar.clone();
			int minsToAdd = 0;
			if(i <= 5) {
				minsToAdd = i;
			} else {
				minsToAdd = (i - 5) * -1;
			}
			cal.add(Calendar.MINUTE, minsToAdd);
			times[i] = cal;
		}
		return times;
	}
	
	//Returns all relevant keys for this object
	public String[] getKeys() {
		Calendar[] cals = getTimeRange();
		String[] keys = new String[NUM_KEYS];
		for(int i = 0; i != NUM_KEYS; ++i) {
			//key format will be ZoneID<sep>Date
			keys[i] = zoneID + NISTClean.KEY_SEP + df.format(cals[i].getTime());
		}
		return keys;
	}
	
	public int getFlow() {
		return flow;
	}
	
}