package com.giant.accountinfo;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class AccountGroup2 {

	private static Logger logger = Logger.getLogger(AccountGroup2.class);

	public static class Map extends Mapper<Object, Text, Text, Text> {
		SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd.HHmmss");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Pattern p = Pattern.compile("\\d{6}\\.\\d*\\.\\d*");

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] cols = value.toString().split("	");
			if (cols.length < 41) {
				logger.error("行解析失败：" + value.toString());
				return;
			}
			String regdate = cols[3];
			String regtime = cols[4]; // String regtime = "2014-09-01 01:00:00";
			String preurl = cols[6];
			String cururl = cols[7];
			String regnip = cols[13];
			String sfzname = cols[17];
			String sfzmd5 = cols[27];

			// String cookieid = "140901.00583063.35308996";
			String cookieid = cols[16];

			String newcookie = "0";
			Matcher m = p.matcher(cookieid);

			try {
				if (m.matches() && cookieid.length() > 14) {
					// cookieid 大于 regtime才是合法的cookie
					if (sdf.parse(cookieid.substring(0, 13)).before(sdf2.parse(regtime)))
						newcookie = cookieid;
				}
			} catch (ParseException e) {
			}

			String newkey = Joiner.on("+").join(regnip, newcookie, sfzname, sfzmd5, regdate, preurl, cururl);
			context.write(new Text(newkey), value);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String regtime = "9999-99-99 99:99:99";
			String uid = "";

			List<Text> tmp = new ArrayList<Text>();
			for (Text text : values) {
				String[] arrs = text.toString().split("	");
				tmp.add(new Text(Joiner.on("\t").join(Arrays.copyOf(arrs, arrs.length - 2))));
				String cregtime = arrs[arrs.length - 1];
				String cuid = arrs[arrs.length - 2];
				if (cregtime != null && cregtime.compareTo(regtime) < 0) {
					regtime = cregtime;
					uid = cuid;
				}
			}

			for (Text text : tmp) {
				context.write(text, new Text(uid+"\t"+regtime));
			}
		}
	}

}
