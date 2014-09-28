package com.giant.accountinfo.rank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * Usage : hadoop jar accountinfo-1.0.0.jar com.giant.accountinfo.RankDict
 * 
 * @author wankun
 * @date 2014年9月25日
 * @version 1.0
 */
public class RankDict {

	public static class Map extends Mapper<Object, Text, Text, IntWritable> {
		private IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] cols = value.toString().split("	");

			String regtime = cols[4];
			context.write(new Text("col4+" + regtime.substring(0, 13)), one);

			String preurl = cols[6];
			context.write(new Text("col6+" + preurl), one);

			String cururl = cols[7];
			context.write(new Text("col7+" + cururl), one);

			String reggametype = cols[8];
			context.write(new Text("col8+" + reggametype), one);

			String regnip = cols[13];
			context.write(new Text("col13+" + regnip), one);

			String regpro = cols[14];
			context.write(new Text("col14+" + regpro), one);

			String regcity = cols[15];
			context.write(new Text("col15+" + regcity), one);

			String cookieid = cols[16];
			context.write(new Text("col16+" + cookieid), one);

			// 根据名字对比困难
			// String sfzname = cols[17];
			// context.write(new Text("col17+" + sfzname), one);

			String regage = cols[21];
			context.write(new Text("col21+" + regage), one);

			String sfzmd5 = cols[27];
			context.write(new Text("col27+" + sfzmd5), one);

			String adid = cols[28];
			context.write(new Text("col28+" + adid), one);

			context.write(new Text("col00+sum rows"), one);

		}
	}

	public static class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int nums = 0;
			for (IntWritable num : values) {
				nums = nums + num.get();
			}

			context.write(key, new IntWritable(nums));
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int nums = 0;
			for (IntWritable num : values) {
				nums = nums + num.get();
			}

			if (nums > 1)
				context.write(key, new IntWritable(nums));
		}
	}

}
