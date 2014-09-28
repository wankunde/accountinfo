package com.giant.accountinfo.rank;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

/**
 * 
 * Usage : hadoop jar accountinfo-1.0.0.jar com.giant.accountinfo.rank.RankMark
 * 
 * @author wankun
 * @date 2014年9月25日
 * @version 1.0
 */
public class RankMark {

	private static Logger logger = Logger.getLogger(RankMark.class);

	public static class Map extends Mapper<Object, Text, Text, DoubleWritable> {

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
			context.write(value, new DoubleWritable(rank));
		}
	}
}
