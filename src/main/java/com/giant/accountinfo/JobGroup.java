package com.giant.accountinfo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * <pre>
 * 组合条件A：注册IP、注册cookie（>0）、注册日期小时、注册身份证号、广告ID，同时相等，则判断为同一自然人
 * 组合条件B：注册IP、（注册cookie=0 or 注册前12位非yymmddhhmmss结构或>注册时间）、注册身份证姓名、注册身份证号、注册日期、注册URL、注册前URL，同时相等，则判断为同一自然人
 * </pre>
 * 
 * Usage : hadoop jar accountinfo-1.0.0.jar com.giant.accountinfo.JobGroup
 * 
 * @author wankun
 * @date 2014年9月24日
 * @version 1.0
 */
public class JobGroup {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] ioArgs = new String[] { "/tmp/wankun/accountinput/", "/tmp/wankun/accountgroup1/",
				"/tmp/wankun/accountgroup2/" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.exit(2);
		}
		JobGroup jobgroup = new JobGroup();
		boolean res1 = false, res2 = false;
		res1 = jobgroup.runjob1(otherArgs[0], otherArgs[1]);
		if (res1)
			res2 = jobgroup.runjob2(otherArgs[1], otherArgs[2]);
		System.exit(res2 ? 0 : 1);
	}

	public boolean runjob1(String input, String output) throws IllegalArgumentException, IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(conf);
		fileSystem.delete(new Path(output), true);

		Job job = Job.getInstance(conf, "AccountInfo 1");
		job.setJarByClass(AccountGroup1.class);
		job.setMapperClass(com.giant.accountinfo.AccountGroup1.Map.class);
		job.setReducerClass(com.giant.accountinfo.AccountGroup1.Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job.waitForCompletion(true);
	}

	public boolean runjob2(String input, String output) throws IllegalArgumentException, IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(conf);
		fileSystem.delete(new Path(output), true);

		Job job = Job.getInstance(conf, "AccountInfo 2");
		job.setJarByClass(AccountGroup2.class);
		job.setMapperClass(com.giant.accountinfo.AccountGroup2.Map.class);
		job.setReducerClass(com.giant.accountinfo.AccountGroup2.Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job.waitForCompletion(true);
	}
}
