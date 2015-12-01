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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

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
	public static final String KEY_SEP = "\t";
	public static final String KEY_SPECIAL = "#";
	public static final String SERGIO_REASON = "2";
	
	private static String LANE_ZONE_MAPPINGS;
	private static String DIR_INPUT;
	private static String DIR_UNSORTED;
	private static String DIR_TEMP;
	private static String DIR_DETECTOR_FILES;

	public static void main(String[] args) throws Exception {
		//I/O Vars Setup
		DIR_INPUT = args[0];
		DIR_TEMP = args[1];
		DIR_UNSORTED = args[2];
		LANE_ZONE_MAPPINGS = DIR_INPUT + "/detector_lane_inventory.csv";
		DIR_DETECTOR_FILES = DIR_INPUT + "/test";
		//Job Setup
		Configuration sergioCleanConfig = new Configuration();
		//add laneID/zone Key/Val Pairs to config
		addLaneIDsToConfig(sergioCleanConfig);

		File folder = new File(DIR_DETECTOR_FILES);
		File[] listOfFiles = folder.listFiles();
		Arrays.sort(listOfFiles);
		System.out.println("Detected Files:");
		for (File file: listOfFiles) {
			String filename = file.getName();
			if(!filename.contains(".csv")){
				continue;
			}
			System.out.println(" - " + file.getName());
		}

		for (File file: listOfFiles) {
			String filename = file.getName();
			if(!filename.contains(".csv")){
				continue;
			}
			System.out.println("Cleaning File: " + filename);
			cleanFile(sergioCleanConfig, filename);
		}
	}
	
	private static void cleanFile(Configuration config, String file) {
		String input = DIR_DETECTOR_FILES + '/' + file;
		String tempOutput = DIR_TEMP + '/' + file;
		String mergedOutput = DIR_UNSORTED + "/unsorted_" + file;
		runTextJob("Sergio Cleaning", config, input, tempOutput, SergioMapper.class, SergioReducer.class);
		mergeOutput(config, tempOutput, mergedOutput);
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

	static FileSystem mergeOutput(Configuration jobConf, String input, String output) {
		FileSystem jobMergeFS = null;
		try {
			Path jobRaw = new Path(input);
			FileSystem jobFS = jobRaw.getFileSystem(jobConf);
			Path jobMerge = new Path(output);
			jobMergeFS = jobRaw.getFileSystem(jobConf);
			org.apache.hadoop.fs.FileUtil.copyMerge(jobFS, jobRaw, jobMergeFS, jobMerge, true, jobConf, "");
		}
		catch(IOException e) {
			e.printStackTrace();
		}

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