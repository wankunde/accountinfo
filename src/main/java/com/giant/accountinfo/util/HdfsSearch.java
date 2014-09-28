package com.giant.accountinfo.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsSearch {

	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.out.println("Usage : HdfsSearch [hdfs path] [pattern] ");
		}
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		FSDataInputStream input = hdfs.open(new Path(args[0]));
		BufferedReader d = new BufferedReader(new InputStreamReader(input));
		String line = d.readLine();
		if (line.contains(args[1]))
			System.out.println(line);
	}
}
