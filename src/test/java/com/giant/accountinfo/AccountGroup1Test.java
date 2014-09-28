package com.giant.accountinfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class AccountGroup1Test {
	MapDriver<Object, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() {
		AccountGroup1.Map mapper = new AccountGroup1.Map();
		AccountGroup1.Reduce reducer = new AccountGroup1.Reduce();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver
				.withInput(
						new LongWritable(),
						new Text(
								"434756447	r5t6o562h242	2014	2014-09-01	2014-09-01 00:00:06			http://reg.ztgame.com/registe/embed/fast_reg.jsp	0	111	72	28	108	1866996844	江西省			冯谦一	1	370827	1971-09-26	42	0	山东省	济宁市	鱼台县	1	2.1605771199.1555738537.660932159.907803220	0	0	0	0	0	0	0000	0000-00-00	0000-00-00 00:00:00	0	0		0		"));
		mapDriver
				.withInput(
						new LongWritable(),
						new Text(
								"347758767	awac0037	2014	2014-09-01	2014-09-01 00:27:26		http://login.ztgame.com/v2/oauth.php?appid=1007&check_validate=0&issso=1&return_url=http://me.ztgame.com/account_manage/loginall&service=user_auth@@	http://reg.ztgame.com/registe/register.jsp?source=giant_site	999	61	158	82	85	1033785941	黑龙江省	牡丹江市	140831.20582211.79639274	徐晴	1	452323	1983-02-23	31	0	广西壮族自治区			1	6.1393502503.417107070.1665577823.1344582835	2502611	61	158	82	85	1033785941	2014	2014-08-31	2014-08-31 21:02:15	21	40	征途2经典版	143	银橙	客户端推送7月"));
		mapDriver
				.withInput(
						new LongWritable(),
						new Text(
								"443756766	huhao199101	2014	2014-09-01	2014-09-01 00:27:44		http://zt2.ztgame.com/activity/ggao/reg.html?ad=2519120	http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=zt2_site&cssurl=f747c2de007c3d94b8fa3bb36ebb008f83ea295aeb6d5c206ac069fba725a5591de0d8291b@@	17	222	191	239	43	3737120555	江苏省	无锡市	140831.23112864.57566048	胡浩	1	320281	1991-01-30	23	0	江苏省	无锡市	江阴市	1	1.1629594532.721360992.542113334.1490131477	2519120	222	191	239	43	3737120555	2014	2014-09-01	2014-09-01 00:26:43	0	17	征途2	139	顺网	客户端launch注册按钮"));
		mapDriver
				.withOutput(
						new Text("1866996844++00+2.1605771199.1555738537.660932159.907803220+0"),
						new Text(
								"434756447	r5t6o562h242	2014	2014-09-01	2014-09-01 00:00:06			http://reg.ztgame.com/registe/embed/fast_reg.jsp	0	111	72	28	108	1866996844	江西省			冯谦一	1	370827	1971-09-26	42	0	山东省	济宁市	鱼台县	1	2.1605771199.1555738537.660932159.907803220	0	0	0	0	0	0	0000	0000-00-00	0000-00-00 00:00:00	0	0		0		"));
		mapDriver
				.withOutput(
						new Text(
								"1033785941+140831.20582211.79639274+00+6.1393502503.417107070.1665577823.1344582835+2502611"),
						new Text(
								"347758767	awac0037	2014	2014-09-01	2014-09-01 00:27:26		http://login.ztgame.com/v2/oauth.php?appid=1007&check_validate=0&issso=1&return_url=http://me.ztgame.com/account_manage/loginall&service=user_auth@@	http://reg.ztgame.com/registe/register.jsp?source=giant_site	999	61	158	82	85	1033785941	黑龙江省	牡丹江市	140831.20582211.79639274	徐晴	1	452323	1983-02-23	31	0	广西壮族自治区			1	6.1393502503.417107070.1665577823.1344582835	2502611	61	158	82	85	1033785941	2014	2014-08-31	2014-08-31 21:02:15	21	40	征途2经典版	143	银橙	客户端推送7月"));
		mapDriver
				.withOutput(
						new Text(
								"3737120555+140831.23112864.57566048+00+1.1629594532.721360992.542113334.1490131477+2519120"),
						new Text(
								"443756766	huhao199101	2014	2014-09-01	2014-09-01 00:27:44		http://zt2.ztgame.com/activity/ggao/reg.html?ad=2519120	http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=zt2_site&cssurl=f747c2de007c3d94b8fa3bb36ebb008f83ea295aeb6d5c206ac069fba725a5591de0d8291b@@	17	222	191	239	43	3737120555	江苏省	无锡市	140831.23112864.57566048	胡浩	1	320281	1991-01-30	23	0	江苏省	无锡市	江阴市	1	1.1629594532.721360992.542113334.1490131477	2519120	222	191	239	43	3737120555	2014	2014-09-01	2014-09-01 00:26:43	0	17	征途2	139	顺网	客户端launch注册按钮"));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		List<Text> v1 = new ArrayList<Text>(
				Arrays.asList(
						new Text(
								"347758767	awac0037	2014	2014-09-01	2014-09-01 00:27:26		http://login.ztgame.com/v2/oauth.php?appid=1007&check_validate=0&issso=1&return_url=http://me.ztgame.com/account_manage/loginall&service=user_auth@@	http://reg.ztgame.com/registe/register.jsp?source=giant_site	999	61	158	82	85	1033785941	黑龙江省	牡丹江市	140831.20582211.79639274	徐晴	1	452323	1983-02-23	31	0	广西壮族自治区			1	6.1393502503.417107070.1665577823.1344582835	2502611	61	158	82	85	1033785941	2014	2014-08-31	2014-08-31 21:02:15	21	40	征途2经典版	143	银橙	客户端推送7月"),
						new Text(
								"347758768	awac0037	2014	2014-09-01	2014-09-01 00:17:26		http://login.ztgame.com/v2/oauth.php?appid=1007&check_validate=0&issso=1&return_url=http://me.ztgame.com/account_manage/loginall&service=user_auth@@	http://reg.ztgame.com/registe/register.jsp?source=giant_site	999	61	158	82	85	1033785941	黑龙江省	牡丹江市	140831.20582211.79639274	徐晴	1	452323	1983-02-23	31	0	广西壮族自治区			1	6.1393502503.417107070.1665577823.1344582835	2502611	61	158	82	85	1033785941	2014	2014-08-31	2014-08-31 21:02:15	21	40	征途2经典版	143	银橙	客户端推送7月")));
		reduceDriver.withInput(new Text(
				"1033785941+140831.20582211.79639274+27+6.1393502503.417107070.1665577823.1344582835+2502611"), v1);
		reduceDriver
		.withOutput(
				new Text(
						"347758767	awac0037	2014	2014-09-01	2014-09-01 00:27:26		http://login.ztgame.com/v2/oauth.php?appid=1007&check_validate=0&issso=1&return_url=http://me.ztgame.com/account_manage/loginall&service=user_auth@@	http://reg.ztgame.com/registe/register.jsp?source=giant_site	999	61	158	82	85	1033785941	黑龙江省	牡丹江市	140831.20582211.79639274	徐晴	1	452323	1983-02-23	31	0	广西壮族自治区			1	6.1393502503.417107070.1665577823.1344582835	2502611	61	158	82	85	1033785941	2014	2014-08-31	2014-08-31 21:02:15	21	40	征途2经典版	143	银橙	客户端推送7月"),
						new Text("347758768	2014-09-01 00:17:26"));
		reduceDriver
		.withOutput(
				new Text(
						"347758768	awac0037	2014	2014-09-01	2014-09-01 00:17:26		http://login.ztgame.com/v2/oauth.php?appid=1007&check_validate=0&issso=1&return_url=http://me.ztgame.com/account_manage/loginall&service=user_auth@@	http://reg.ztgame.com/registe/register.jsp?source=giant_site	999	61	158	82	85	1033785941	黑龙江省	牡丹江市	140831.20582211.79639274	徐晴	1	452323	1983-02-23	31	0	广西壮族自治区			1	6.1393502503.417107070.1665577823.1344582835	2502611	61	158	82	85	1033785941	2014	2014-08-31	2014-08-31 21:02:15	21	40	征途2经典版	143	银橙	客户端推送7月"),
						new Text("347758768	2014-09-01 00:17:26"));
		reduceDriver.runTest();
	}

}
