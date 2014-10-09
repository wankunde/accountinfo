package com.giant.accountinfo.rank;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

/**
 * <pre>
 * 数值化校对
 * 
 * 调整目标：减小(rank-colpr)的标准差
 * 	标准差很大，但是值很小--> 调整系数
 * 
 * 如 (rank-colpr) 远小于0 : 
 * 		a. 减小colpr系数
 * 		b. 调整colpr计算函数，增大colpr值 
 * 
 * 
 * 如 (rank-colpr) 远大于0 : 
 * 		a. 减小colpr系数
 * 		b. 调整colpr计算函数，减小colpr值
 * </pre>
 * 
 * @author wankun
 * @date 2014年9月25日
 * @version 1.0
 */
public class RankMark {

	private static Logger logger = Logger.getLogger(RankMark.class);

	public static class Map extends Mapper<Object, Text, Text, Text> {

		java.util.Map<String, Integer> dict = new HashMap<String, Integer>();
		int allrows = 0;

		@Override
		protected void setup(Context context) throws IOException {
			try {
				List<String> lines = Files.readLines(new File("DICT"), Charsets.UTF_8);
				for (String line : lines) {
					String[] fields = line.split("	");
					if (fields != null && fields.length > 1)
						dict.put(fields[0], Integer.parseInt(fields[1]));
				}
			} catch (Exception e) {
				logger.error("字典数据加载失败", e);
			}

			allrows = dict.get("col00+sum rows");
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] cols = value.toString().split("	");

			if (cols.length < 39) {
				logger.error("行解析失败：" + value.toString());
				return;
			}

			String regtime = "col4+" + cols[4].substring(0, 13);
			double x4 = RankFunction
					.getRegtimeRank(regtime, dict.get(regtime) == null ? 1 : dict.get(regtime), allrows);

			String preurl = "col6+" + cols[6];
			double x6 = RankFunction.getPreurlRank(preurl, dict.get(preurl) == null ? 1 : dict.get(preurl), allrows);

			String cururl = "col7+" + cols[7];
			double x7 = RankFunction.getCurlurlRank(cururl, dict.get(cururl) == null ? 1 : dict.get(cururl), allrows);

			String reggametype = "col8+" + cols[8];
			double x8 = RankFunction.getReggametypeRank(reggametype,
					dict.get(reggametype) == null ? 1 : dict.get(reggametype), allrows);

			String regnip = "col13+" + cols[13];
			double x13 = RankFunction.getRegnipRank(regnip, dict.get(regnip) == null ? 1 : dict.get(regnip), allrows);

			String cookieid = "col16+" + cols[16];
			double x16 = RankFunction.getCookieidRank(cookieid, dict.get(cookieid) == null ? 1 : dict.get(cookieid),
					allrows);

			String regage = "col21+" + cols[21];
			double x21 = RankFunction.getRegageRank(cols[21], dict.get(regage) == null ? 1 : dict.get(regage), allrows);

			String sfzmd5 = "col27+" + cols[27];
			double x27 = RankFunction.getSfzmd5Rank(sfzmd5, dict.get(sfzmd5) == null ? 1 : dict.get(sfzmd5), allrows);

			double[] xs = { x4, x6, x7, x8, x13, x16, x21, x27 };
			double rank = RankFunction.getAllRank(xs);
			context.write(new Text("output"), new Text(value + "\t" + rank));

			context.write(new Text("col4"), new Text(x4 + "\t" + rank));
			context.write(new Text("col6"), new Text(x6 + "\t" + rank));
			context.write(new Text("col7"), new Text(x7 + "\t" + rank));
			context.write(new Text("col8"), new Text(x8 + "\t" + rank));
			context.write(new Text("col13"), new Text(x13 + "\t" + rank));
			context.write(new Text("col16"), new Text(x16 + "\t" + rank));
			context.write(new Text("col21"), new Text(x21 + "\t" + rank));
			context.write(new Text("col27"), new Text(x27 + "\t" + rank));
		}
	}

	/* 这里通过判断key值将所有数据分布到两个reducer中 */
	public static class Partition extends Partitioner<Text, Text> {
		public int getPartition(Text key, Text value, int numPartitions) {
			int res = 0;
			if (numPartitions >= 9)
				switch (key.toString()) {
				case "output":
					res = 0;
					break;
				case "col4":
					res = 1;
					break;
				case "col6":
					res = 2;
					break;
				case "col7":
					res = 3;
					break;
				case "col8":
					res = 4;
					break;
				case "col13":
					res = 5;
					break;
				case "col16":
					res = 6;
					break;
				case "col21":
					res = 7;
					break;
				case "col27":
					res = 8;
					break;
				default:
					res = 0;
				}
			return res;
		}

	}

	public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

		// 将结果输出到多个文件或多个文件夹
		private MultipleOutputs<Text, NullWritable> mos;

		// 创建对象
		protected void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, NullWritable>(context);
		}

		// 关闭对象
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}

		List<Double> col_ranks = new ArrayList<>();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				if (key.equals(new Text("output"))) {
					mos.write("output", value, NullWritable.get());
				} else if (key.toString().startsWith("col")) {
					String[] ss = value.toString().split("\t");
					double colPr = Double.parseDouble(ss[0]);
					double rank = Double.parseDouble(ss[1]);
					// 计算列估计和最后评估值的差异程度
					if (rank > 0)
						col_ranks.add((rank - colPr) / rank);
					// mos.write("overview", new Text(key.toString() + "-test" +
					// "\t" + rank + "\t" + colPr),
					// NullWritable.get());
				}
			}

			if (col_ranks.size() > 0) {
				// 计算list的均值，以及标准差
				double avg = avg(col_ranks);
				double stdavg = stdavg(col_ranks);
				mos.write("overview", new Text(key.toString() + "\t(rank-colpr)均值：" + avg + "\t标准差：" + stdavg),
						NullWritable.get());
			}
		}

		public double avg(List<Double> ds) {
			if (ds == null)
				return 0D;
			if (ds.size() == 0)
				return 0D;
			double sum = 0D;
			for (int i = 0; i < ds.size(); i++) {
				sum += ds.get(i);
			}
			return sum / ds.size();
		}

		public double avg(double[] ds) {
			if (ds == null)
				return 0D;
			if (ds.length == 0)
				return 0D;
			double sum = 0D;
			for (int i = 0; i < ds.length; i++) {
				sum += ds[i];
			}
			return sum / ds.length;
		}

		public double stdavg(List<Double> ds) {
			if (ds == null)
				return 0D;
			if (ds.size() == 0)
				return 0D;
			double avg = 0D;
			for (int i = 0; i < ds.size(); i++) {
				avg += ds.get(i);
			}
			avg = avg / ds.size();
			double sum = 0D;
			for (int i = 0; i < ds.size(); i++) {
				sum += (ds.get(i) - avg) * (ds.get(i) - avg);
			}
			sum /= ds.size();
			return Math.sqrt(sum);
		}

		public double stdavg(double[] ds) {
			if (ds == null)
				return 0D;
			if (ds.length == 0)
				return 0D;
			double avg = 0D;
			for (int i = 0; i < ds.length; i++) {
				avg += ds[i];
			}
			avg = avg / ds.length;
			double sum = 0D;
			for (int i = 0; i < ds.length; i++) {
				sum += (ds[i] - avg) * (ds[i] - avg);
			}
			sum /= ds.length;
			return Math.sqrt(sum);
		}

	}
}
