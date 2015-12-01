package com.nistfortunetellers.cleaning;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

class Measurement {
	
	private SimpleDateFormat df = new SimpleDateFormat(NISTClean.DATE_FORMAT);
	private String laneID;
	private int zoneID;
	private Calendar calendar;
	
	private static final int DEFAULT_ZONE = 999999;
	
	//Creates a Measurment object based off of a line.
	public Measurement(Configuration config, String line) {
		
		String[] splits = line.split(",");
		
		/* Retrieve Needed Values */
		/* Lane ID */
		laneID = splits[0];
		//Get the Zone ID, putting it into a default if not found.
		zoneID = config.getInt(laneID, DEFAULT_ZONE);
		/* Date */
		// ex. 2006-09-01 00:00:07-04. 
		//The below code cuts off the above example, to be an easily parsable string
		//like 2006-09-01 00:00
		Date date;
		try {
			String dateStr = splits[1].substring(0, 16);
			date = df.parse(dateStr);
		} catch (IndexOutOfBoundsException e) {
			date = new Date();
			//throw new IllegalArgumentException("Unable to Parse Date.");
		} catch (ParseException e) {
			date = new Date();
			//throw new IllegalArgumentException("Unable to Parse Date.");
		}
		calendar = GregorianCalendar.getInstance();
		calendar.setTime(date);
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
			if(i <= NUM_KEYS/2) {
				minsToAdd = i;
			} else {
				minsToAdd = (i - NUM_KEYS/2) * -1;
			}
			cal.add(Calendar.MINUTE, minsToAdd);
			times[i] = cal;
		}
		return times;
	}
	
	private String reducerKeyFromCalendar(Calendar calendar) {
		return zoneID + NISTClean.KEY_SEP + df.format(calendar.getTime());
	}
	
	/** Special Mapper key to identify self. */
	public Text selfMapKey() {
		return new Text(reducerKeyFromCalendar(calendar));
	}
	
	//Returns all relevant keys for this object
	public String[] timeKeys() {
		Calendar[] cals = getTimeRange();
		String[] keys = new String[cals.length];
		for(int i = 0; i != cals.length; ++i) {
			//key format will be ZoneID<sep>Date
			keys[i] = reducerKeyFromCalendar(cals[i]);
		}
		return keys;
	}
	
}