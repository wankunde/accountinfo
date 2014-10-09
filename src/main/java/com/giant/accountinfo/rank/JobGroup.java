package com.giant.accountinfo.rank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

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

	private static Logger logger = Logger.getLogger(JobGroup.class);
	public static Configuration conf = new Configuration();
	public static FileSystem fileSystem = null;

	public static void main(String[] args) throws Exception {
		String[] ioArgs = new String[] { "/tmp/wankun/accountinput/", "/tmp/wankun/accountdict/",
				"/tmp/wankun/accountrank/" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.exit(2);
		}
		fileSystem = FileSystem.get(conf);
		JobGroup rankjobs = new JobGroup();
		boolean res1 = false, res2 = false;
		res1 = rankjobs.generateDict(otherArgs[0], otherArgs[1]);
		if (res1)
			res2 = rankjobs.generateRank(otherArgs[0], otherArgs[2], ioArgs[1] + "part-r-00000#DICT");

		// 计算完毕后，输出各个字段距离最终值的平均偏差
		if (res2) {
			StringBuffer content = new StringBuffer("-------评分分布情况---------\n");
			FileStatus[] status = fileSystem.listStatus(new Path(ioArgs[2]));
			for (FileStatus st : status) {
				Path temp = st.getPath();
				if (temp.getName().toString().startsWith("overview")) {
					FSDataInputStream in = fileSystem.open(temp);
					BufferedReader reader = new BufferedReader(new InputStreamReader(in));
					String line = reader.readLine();

					if (line != null && line.length() > 0)
						content.append(line+"\n");
				}

				if (temp.getName().toString().startsWith("part-r")) {
					fileSystem.delete(temp, true);
				}
			}

			logger.info(content.toString());
		}
		System.exit(res2 ? 0 : 1);
	}

	public boolean generateDict(String input, String output) throws IllegalArgumentException, IOException,
			ClassNotFoundException, InterruptedException {
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

	public boolean generateRank(String input, String output, String dict) throws IllegalArgumentException, IOException,
			ClassNotFoundException, InterruptedException, URISyntaxException {
		fileSystem.delete(new Path(output), true);

		Job job = Job.getInstance(conf, "generate row rank");
		job.setJarByClass(RankMark.class);
		job.setMapperClass(com.giant.accountinfo.rank.RankMark.Map.class);
		job.setPartitionerClass(com.giant.accountinfo.rank.RankMark.Partition.class);
		job.setReducerClass(com.giant.accountinfo.rank.RankMark.Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		MultipleOutputs.addNamedOutput(job, "output", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "overview", TextOutputFormat.class, Text.class, NullWritable.class);

		job.setNumReduceTasks(9);

		job.addCacheFile(new URI(dict));

		return job.waitForCompletion(true);
	}
}