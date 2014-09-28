package com.giant.accountinfo.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.giant.accountinfo.rank.RankFunction;
import com.google.common.base.Joiner;

/**
 * Usage : hadoop jar accountinfo-1.0.0.jar
 * com.giant.accountinfo.util.AccountMarkInfo [str]
 * 
 * @author wankun
 * @date 2014年9月26日
 * @version 1.0
 */
public class AccountMarkInfo {

	public static void main(String[] args) throws IOException {
		if (args.length < 1) {
			System.out.println("Usage : AccountMarkInfo [pattern] ");
		}

		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);

		// 读取字典信息
		Map<String, Integer> dict = new HashMap<String, Integer>();
		int allrows = 0;

		BufferedReader dictreader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(
				"/tmp/wankun/accountdict/part-r-00000"))));
		String str = null;
		while ((str = dictreader.readLine()) != null) {
			String[] fields = str.split("	");
			if (fields != null && fields.length > 1)
				dict.put(fields[0], Integer.parseInt(fields[1]));
		}
		allrows = dict.get("col00+sum rows");

		// 读取原始文件内容，并匹配
		BufferedReader srcreader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(
				"/tmp/wankun/accountinput/b"))));
		str = null;
		while ((str = srcreader.readLine()) != null) {
			if (str.contains(args[0])) {
				String[] cols = str.split("	");
				String regtime = "col4+" + cols[4].substring(0, 13);
				double x4 = RankFunction.getRegtimeRank(regtime, dict.get(regtime) == null ? 1 : dict.get(regtime),
						allrows);

				String preurl = "col6+" + cols[6];
				double x6 = RankFunction
						.getPreurlRank(preurl, dict.get(preurl) == null ? 1 : dict.get(preurl), allrows);

				String cururl = "col7+" + cols[7];
				double x7 = RankFunction.getCurlurlRank(cururl, dict.get(cururl) == null ? 1 : dict.get(cururl),
						allrows);

				String reggametype = "col8+" + cols[8];
				double x8 = RankFunction.getReggametypeRank(reggametype,
						dict.get(reggametype) == null ? 1 : dict.get(reggametype), allrows);

				String regnip = "col13+" + cols[13];
				double x13 = RankFunction.getRegnipRank(regnip, dict.get(regnip) == null ? 1 : dict.get(regnip),
						allrows);

				String cookieid = "col16+" + cols[16];
				double x16 = RankFunction.getCookieidRank(cookieid,
						dict.get(cookieid) == null ? 1 : dict.get(cookieid), allrows);

				String regage = "col21+" + cols[21];
				double x21 = RankFunction.getRegageRank(cols[21], dict.get(regage) == null ? 1 : dict.get(regage),
						allrows);

				String sfzmd5 = "col27+" + cols[27];
				double x27 = RankFunction.getSfzmd5Rank(sfzmd5, dict.get(sfzmd5) == null ? 1 : dict.get(sfzmd5),
						allrows);

				System.out.println("line:" + str);
				System.out.println("marks : "
						+ Joiner.on("\t").join("regtime:" + x4, "preurl:" + x6, "cururl:" + x7, "reggametype:" + x8,
								"regnip:" + x13, "cookieid:" + x16, "regage:" + x21, "sfzmd5:" + x27));
				double[] xs = { x4, x6, x7, x8, x13, x16, x21, x27 };
				System.out.println("last marks : " + RankFunction.getAllRank(xs));
			}
		}
	}
}
