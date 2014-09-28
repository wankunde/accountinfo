package com.giant.accountinfo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

import com.google.common.base.Joiner;

public class Test2 {

	public static void main(String[] args) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd.HHmmss");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Pattern p = Pattern.compile("\\d{6}\\.\\d*\\.\\d*");
		
		String line ="419756653	yns2014a	2014	2014-09-01	2014-09-01 01:00:00		http://act.jd.ztgame.com/jd/index.html?ad=2344690	http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=jd_site&cssurl=11f98bb8fbd2243e3ca35737f1e0f6a48e185ecc3a7a632b0068117914a75fbf502498d51c8@@	40	121	25	91	229	2031705061	河北省	张家口市	140901.00583063.35308996	啊啊	2	130728	1989-03-02	25	0	河北省	张家口市	怀安县	1	4.1703859705.927002318.1315651126.1975871234	2344690	121	25  91	229	2031705061	2014	2014-09-01	2014-09-01 00:58:30	0	0	征途2经典版	13	百度	网页专区标题	419756653	2014-09-01 01:00:00";
		String[] cols = line.split("	");
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
		System.out.println(newkey);
		System.out.println(line);
	}

}
