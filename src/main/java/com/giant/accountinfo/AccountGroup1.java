package com.giant.accountinfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;

/**
 * <pre>
 * 组合条件A：注册IP、注册cookie（>0）、注册日期小时、注册身份证号、广告ID，同时相等，则判断为同一自然人
 * 组合条件B：注册IP、（注册cookie=0 or 注册前12位非yymmddhhmmss结构或>注册时间）、注册身份证姓名、注册身份证号、注册日期、注册URL、注册前URL，同时相等，则判断为同一自然人
 * 
 * 
 * </pre>
 * <p>
 * Description:
 * </p>
 * 
 * @author wankun
 * @date 2014年9月24日
 * @version 1.0
 */
public class AccountGroup1 {

	private static Logger logger = Logger.getLogger(AccountGroup1.class);

	public static class Map extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] cols = value.toString().split("	");
			if (cols.length < 39) {
				logger.error("行解析失败：" + value.toString());
				return;
			}
			String regnip = cols[13];
			String cookieid = cols[16];
			String regtime = cols[4];
			if (regtime.length() < 13) {
				logger.error("行解析失败：" + value.toString());
				return;
			}
			String sfzmd5 = cols[27];
			String adid = cols[28];
			String newkey = Joiner.on("+").join(regnip, cookieid, regtime.substring(11, 13), sfzmd5, adid);
			context.write(new Text(newkey), value);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String regtime = "9999-99-99 99:99:99";
			String uid = "";

			List<Text> tmp = new ArrayList<Text>();
			for (Text text : values) {
				tmp.add(new Text(text));

				String[] arrs = text.toString().split("	");
				String cregtime = arrs[4];
				String cuid = arrs[0];
				if (cregtime != null && cregtime.compareTo(regtime) < 0) {
					regtime = cregtime;
					uid = cuid;
				}
			}

			for (Text text : tmp) {
				context.write(text, new Text(uid + "\t" + regtime));
			}
		}
	}

}
