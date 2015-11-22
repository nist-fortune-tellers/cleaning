/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nistfortunetellers.cleaning;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NISTClean {
	
	//in the future, will just be a folder, but for now it's an individual item.
	
	public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm";
	public static final String KEY_SEP = "*";
	public static final String KEY_SPECIAL = "#";
	public static final String SERGIO_REASON = "2";
	
	private static String LANE_ZONE_MAPPINGS = "";

	public static void main(String[] args) throws Exception {
		//I/O Vars Setup
		final String DIR_INPUT = args[0];
		final String DIR_TEMP = args[1];
		final String DIR_OUTPUT = args[2];
		LANE_ZONE_MAPPINGS = DIR_INPUT + "/detector_lane_inventory.csv";
		final String DIR_DETECTOR_FILES = DIR_INPUT + "/test";
		
		//Job Setup
		Configuration sergioCleanConfig = new Configuration();
		//add laneID/zone Key/Val Pairs to config
		addLaneIDsToConfig(sergioCleanConfig);
		final String file = "cleaning_test_small_06_11.csv";
		final String finalOutputFileName = DIR_OUTPUT + "/" + file.replace(".csv", "_NIST-3.txt").replace("test", "subm");		
		String input = DIR_DETECTOR_FILES + '/' + file;
		String tempOutput = DIR_TEMP + '/' + file;
		String tempMergedOutput = DIR_TEMP + "/notsorted_" + file;
		String finalOutput = DIR_OUTPUT + "/" + file;
		runTextJob("Sergio Cleaning", sergioCleanConfig, input, tempOutput, SergioMapper.class, SergioReducer.class);
		mergeOutput(sergioCleanConfig, tempOutput, tempMergedOutput);
		sortOutput(tempMergedOutput, finalOutputFileName );
	}
	
	private static void sortOutput(String input, String output) {
		BufferedReader br = null;
		BufferedWriter bw = null;
		String line = "";
		try {
			br = new BufferedReader(new FileReader(input));
			// Get the number of lines in the file
			LineNumberReader  lnr = new LineNumberReader(new FileReader(input));
			lnr.skip(Long.MAX_VALUE);
			int numberOfLines = lnr.getLineNumber(); //Add 1 because line index starts at 0
			// Finally, the LineNumberReader object should be closed to prevent resource leak
			lnr.close();	
			
			System.out.println("The number of lines is : " + numberOfLines);
			
			
			LaneSorter[] lane_arr = new LaneSorter[numberOfLines];
			for(int i = 0; i != numberOfLines; ++i) {
				line = br.readLine();
				lane_arr[i] = new LaneSorter(line);
			}
			br.close();
			Arrays.sort(lane_arr);
			
			// Write file sorted by lane_id to output
			bw = new BufferedWriter(new FileWriter(output));
			for(LaneSorter ls : lane_arr) {
				bw.write(ls.toString());
				bw.newLine();
			}
			bw.close();
			lane_arr = null;

			
			// Now sort by measurement_start
			br = new BufferedReader(new FileReader(output));
			SimpleDateFormat sf = new SimpleDateFormat(DATE_FORMAT);
						
			DateSorter[] arr = new DateSorter[numberOfLines];
			for(int i = 0; i != numberOfLines; ++i) {
				line = br.readLine();
				arr[i] = new DateSorter(sf, line);
			}
			br.close();
			Arrays.sort(arr);
			
			// Finally write completed sorted file to 
			bw = new BufferedWriter(new FileWriter(output));
			for(DateSorter ds : arr) {
				bw.write(ds.toString());
				bw.newLine();
			}
			bw.close();
			arr = null;

			
		} catch (IOException e) {
			e.printStackTrace();
		}	
		
	}

	/** Runs a Job that is Text in and Out, and TextInput in and out, too! */
	@SuppressWarnings({ "deprecation", "rawtypes" })
	static void runTextJob(String jobName, Configuration jobConfig,
			String inputPath, String outputPath,
			Class<? extends Mapper> mapper, Class<? extends Reducer> reducer) {
		try {
			Job genericJob = new Job(jobConfig, jobName);
			// DEBUG
			//genericJob.setNumReduceTasks(0);
			// END DEBUG
			genericJob.setJarByClass(NISTClean.class);
			genericJob.setOutputKeyClass(Text.class);
			genericJob.setOutputValueClass(Text.class);
			genericJob.setMapperClass(mapper);
			genericJob.setReducerClass(reducer);
			genericJob.setInputFormatClass(TextInputFormat.class);
			genericJob.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(genericJob, new Path(inputPath));
			FileOutputFormat.setOutputPath(genericJob, new Path(outputPath));
			genericJob.waitForCompletion(true);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	static FileSystem mergeOutput(Configuration jobConf, String input, String output) throws IOException {
		Path jobRaw = new Path(input);
		FileSystem jobFS = jobRaw.getFileSystem(jobConf);
		Path jobMerge = new Path(output);
		FileSystem jobMergeFS = jobRaw.getFileSystem(jobConf);
		org.apache.hadoop.fs.FileUtil.copyMerge(jobFS, jobRaw, jobMergeFS, jobMerge, true, jobConf, "");
		return jobMergeFS;
	}

	static void addLaneIDsToConfig(Configuration config) {
		BufferedReader br = null;
		String line = "";
		String splitChar = ",";
		try {

			br = new BufferedReader(new FileReader(LANE_ZONE_MAPPINGS));
			//Skip First Line, since it is the keys.
			br.readLine();
			//Read In All Lines In File
			while ((line = br.readLine()) != null) {
				//Split by Comma
				String[] splits = line.split(splitChar);
				
				/* Parse Relevant Docs */
				//will be used as key
				String laneID = ""; 
				//associated val for key
				int zoneID = 0; 
				//make sure we aren't just on a blank line
				if (splits.length >= 2) {
					laneID = splits[0].trim();
					try {
						zoneID = Integer.parseInt(splits[1]);
					} catch (NumberFormatException e) {
						//If format doesn't work out so well, no worries. Just skip.
						//System.out.println("--Skipped line Line: " + line.replace('\n', ' '));
						continue;
					}
				} else {
					//probably means we're done, but continue just in case.
					continue;
				}
				//DEBUG: Print Output
				System.out.println("LaneID: " + laneID + ", zoneID: " + zoneID);
				//Now actually set the config!
				config.setInt(laneID, zoneID);
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}
}

class DateSorter implements Comparable<DateSorter>  {
	
	String line;
	Date date;
	
	public DateSorter(SimpleDateFormat df, String line) {
		this.line = line;
		System.out.println(line);
		String[] splits = line.split("\t");
		try {
			String dateStr = splits[1];
			date = df.parse(dateStr);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public int compareTo(DateSorter o) {
		return date.compareTo(o.getDate());
	}
	
	public Date getDate() {
		return date;
	}

	@Override
	public String toString() {
		return line;
	}
	
}

class LaneSorter implements Comparable<LaneSorter> {
	
	String line;
	Integer lane_id;
	
	public LaneSorter(String line) {
		this.line = line;
		String[] splits = line.split("\t");
		lane_id = Integer.parseInt(splits[0]);
	}

	@Override
	public int compareTo(LaneSorter o) {
		return lane_id.compareTo(o.getLaneID());
	}
	
	public int getLaneID() {
		return lane_id;
	}

	@Override
	public String toString() {
		return line;
	}
	
}