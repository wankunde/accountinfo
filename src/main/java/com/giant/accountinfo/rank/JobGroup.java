package com.giant.accountinfo.rank;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * job1 : 生成各个字段的重复数据的数据字典
 * 
 * job2 : 根据字段的值，重复数据量来计算字段的rank权值
 * 
 * Usage : hadoop jar accountinfo-1.0.0.jar com.giant.accountinfo.rank.JobGroup
 * 
 * @author wankun
 * @date 2014年9月26日
 * @version 1.0
 */
public class JobGroup {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] ioArgs = new String[] { "/tmp/wankun/accountinput/", "/tmp/wankun/accountdict/",
				"/tmp/wankun/accountrank/" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.exit(2);
		}
		JobGroup rankjobs = new JobGroup();
		boolean res1 = false, res2 = false;
		res1 = rankjobs.generateDict(otherArgs[0], otherArgs[1]);
		if (res1)
			res2 = rankjobs.generateRank(otherArgs[0], otherArgs[2]);
		System.exit(res2 ? 0 : 1);
	}

	public boolean generateDict(String input, String output) throws IllegalArgumentException, IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(conf);
		fileSystem.delete(new Path(output), true);

		Job job = Job.getInstance(conf, "generate account dict");
		job.setJarByClass(RankDict.class);
		job.setMapperClass(com.giant.accountinfo.rank.RankDict.Map.class);
		 job.setCombinerClass(com.giant.accountinfo.rank.RankDict.Combine.class);
		job.setReducerClass(com.giant.accountinfo.rank.RankDict.Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job.waitForCompletion(true);
	}

	public boolean generateRank(String input, String output) throws IllegalArgumentException, IOException,
			ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		final FileSystem fileSystem = FileSystem.get(conf);
		fileSystem.delete(new Path(output), true);

		Job job = Job.getInstance(conf, "generate row rank");
		job.setJarByClass(RankMark.class);
		job.setMapperClass(com.giant.accountinfo.rank.RankMark.Map.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.addCacheFile(new URI("/tmp/wankun/accountdict/part-r-00000#DICT"));

		return job.waitForCompletion(true);
	}
}