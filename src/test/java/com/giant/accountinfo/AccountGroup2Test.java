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

public class AccountGroup2Test {
	MapDriver<Object, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() {
		AccountGroup2.Map mapper = new AccountGroup2.Map();
		AccountGroup2.Reduce reducer = new AccountGroup2.Reduce();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver
				.withInput(
						new LongWritable(),
						new Text(
								"421758628	yns2014c	2014	2014-09-01	2014-09-01 01:06:06			http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=jd_site&cssurl=11f98bb8fbd2243e3ca35737f1e0f6a48e185ecc3a7a632b0068117914a75fbf502498d51c8@@	40	121	25	91	229	2031705061	河北省	张家口市	140901.00583063.35308996	啊啊	2	130728	1989-03-02	25	0	河北省	张家口市	怀安县	1	4.1703859705.927002318.1315651126.1975871234	2344690	121	25	91	229	2031705061	20142014-09-01	2014-09-01 01:03:10	1	0	征途2经典版	13	百度	网页专区标题	419756653	2014-09-01 01:00:00"));
		mapDriver
				.withInput(
						new LongWritable(),
						new Text(
								"420757036	yns2014b	2014	2014-09-01	2014-09-01 01:02:56			http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=jd_site&cssurl=11f98bb8fbd2243e3ca35737f1e0f6a48e185ecc3a7a632b0068117914a75fbf502498d51c8@@	40	121	25	91	229	2031705061	河北省	张家口市	140901.00583063.35308996	啊啊	2	130728	1989-03-02	25	0	河北省	张家口市	怀安县	1	4.1703859705.927002318.1315651126.1975871234	2344690	121	25	91	229	2031705061	20142014-09-01	2014-09-01 01:01:23	1	0	征途2经典版	13	百度	网页专区标题	419756653	2014-09-01 01:00:00"));
		mapDriver
				.withInput(
						new LongWritable(),
						new Text(
								"419756653	yns2014a	2014	2014-09-01	2014-09-01 01:00:00		http://act.jd.ztgame.com/jd/index.html?ad=2344690	http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=jd_site&cssurl=11f98bb8fbd2243e3ca35737f1e0f6a48e185ecc3a7a632b0068117914a75fbf502498d51c8@@	40	121	25	91	229	2031705061	河北省	张家口市	140901.00583063.35308996	啊啊	2	130728	1989-03-02	25	0	河北省	张家口市	怀安县	1	4.1703859705.927002318.1315651126.1975871234	2344690	121	25  91	229	2031705061	2014	2014-09-01	2014-09-01 00:58:30	0	0	征途2经典版	13	百度	网页专区标题	419756653	2014-09-01 01:00:00"));
		mapDriver
				.withOutput(
						new Text(
								"2031705061+140901.00583063.35308996+啊啊+4.1703859705.927002318.1315651126.1975871234+2014-09-01++http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=jd_site&cssurl=11f98bb8fbd2243e3ca35737f1e0f6a48e185ecc3a7a632b0068117914a75fbf502498d51c8@@"),
						new Text(
								"421758628	yns2014c	2014	2014-09-01	2014-09-01 01:06:06			http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=jd_site&cssurl=11f98bb8fbd2243e3ca35737f1e0f6a48e185ecc3a7a632b0068117914a75fbf502498d51c8@@	40	121	25	91	229	2031705061	河北省	张家口市	140901.00583063.35308996	啊啊	2	130728	1989-03-02	25	0	河北省	张家口市	怀安县	1	4.1703859705.927002318.1315651126.1975871234	2344690	121	25	91	229	2031705061	20142014-09-01	2014-09-01 01:03:10	1	0	征途2经典版	13	百度	网页专区标题	419756653	2014-09-01 01:00:00"));
		mapDriver
				.withOutput(
						new Text(
								"2031705061+140901.00583063.35308996+啊啊+4.1703859705.927002318.1315651126.1975871234+2014-09-01++http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=jd_site&cssurl=11f98bb8fbd2243e3ca35737f1e0f6a48e185ecc3a7a632b0068117914a75fbf502498d51c8@@"),
						new Text(
								"420757036	yns2014b	2014	2014-09-01	2014-09-01 01:02:56			http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=jd_site&cssurl=11f98bb8fbd2243e3ca35737f1e0f6a48e185ecc3a7a632b0068117914a75fbf502498d51c8@@	40	121	25	91	229	2031705061	河北省	张家口市	140901.00583063.35308996	啊啊	2	130728	1989-03-02	25	0	河北省	张家口市	怀安县	1	4.1703859705.927002318.1315651126.1975871234	2344690	121	25	91	229	2031705061	20142014-09-01	2014-09-01 01:01:23	1	0	征途2经典版	13	百度	网页专区标题	419756653	2014-09-01 01:00:00"));
		mapDriver
				.withOutput(
						new Text(
								"2031705061+140901.00583063.35308996+啊啊+4.1703859705.927002318.1315651126.1975871234+2014-09-01+http://act.jd.ztgame.com/jd/index.html?ad=2344690+http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=jd_site&cssurl=11f98bb8fbd2243e3ca35737f1e0f6a48e185ecc3a7a632b0068117914a75fbf502498d51c8@@"),
						new Text(
								"419756653	yns2014a	2014	2014-09-01	2014-09-01 01:00:00		http://act.jd.ztgame.com/jd/index.html?ad=2344690	http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=jd_site&cssurl=11f98bb8fbd2243e3ca35737f1e0f6a48e185ecc3a7a632b0068117914a75fbf502498d51c8@@	40	121	25	91	229	2031705061	河北省	张家口市	140901.00583063.35308996	啊啊	2	130728	1989-03-02	25	0	河北省	张家口市	怀安县	1	4.1703859705.927002318.1315651126.1975871234	2344690	121	25  91	229	2031705061	2014	2014-09-01	2014-09-01 00:58:30	0	0	征途2经典版	13	百度	网页专区标题	419756653	2014-09-01 01:00:00"));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		List<Text> v1 = new ArrayList<Text>(
				Arrays.asList(
						new Text(
								"421758628	yns2014c	2014	2014-09-01	2014-09-01 01:06:06			http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=jd_site&cssurl=11f98bb8fbd2243e3ca35737f1e0f6a48e185ecc3a7a632b0068117914a75fbf502498d51c8@@	40	121	25	91	229	2031705061	河北省	张家口市	140901.00583063.35308996	啊啊	2	130728	1989-03-02	25	0	河北省	张家口市	怀安县	1	4.1703859705.927002318.1315651126.1975871234	2344690	121	25	91	229	2031705061	20142014-09-01	2014-09-01 01:03:10	1	0	征途2经典版	13	百度	网页专区标题	421758628	2014-09-01 01:06:06"),
						new Text(
								"420757036	yns2014b	2014	2014-09-01	2014-09-01 01:02:56			http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=jd_site&cssurl=11f98bb8fbd2243e3ca35737f1e0f6a48e185ecc3a7a632b0068117914a75fbf502498d51c8@@	40	121	25	91	229	2031705061	河北省	张家口市	140901.00583063.35308996	啊啊	2	130728	1989-03-02	25	0	河北省	张家口市	怀安县	1	4.1703859705.927002318.1315651126.1975871234	2344690	121	25	91	229	2031705061	20142014-09-01	2014-09-01 01:01:23	1	0	征途2经典版	13	百度	网页专区标题	419756653	2014-09-01 01:00:00"),
						new Text(
								"419756653	yns2014a	2014	2014-09-01	2014-09-01 01:00:00		http://act.jd.ztgame.com/jd/index.html?ad=2344690	http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=jd_site&cssurl=11f98bb8fbd2243e3ca35737f1e0f6a48e185ecc3a7a632b0068117914a75fbf502498d51c8@@	40	121	25	91	229	2031705061	河北省	张家口市	140901.00583063.35308996	啊啊	2	130728	1989-03-02	25	0	河北省	张家口市	怀安县	1	4.1703859705.927002318.1315651126.1975871234	2344690	121	25  91	229	2031705061	2014	2014-09-01	2014-09-01 00:58:30	0	0	征途2经典版	13	百度	网页专区标题	419756653	2014-09-01 01:00:00")));
		reduceDriver
				.withInput(
						new Text(
								"2031705061+140901.00583063.35308996+啊啊+4.1703859705.927002318.1315651126.1975871234+2014-09-01+http://act.jd.ztgame.com/jd/index.html?ad=2344690+http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=jd_site&cssurl=11f98bb8fbd2243e3ca35737f1e0f6a48e185ecc3a7a632b0068117914a75fbf502498d51c8@@"),
						v1);
		reduceDriver
				.withOutput(
						new Text(
								"421758628	yns2014c	2014	2014-09-01	2014-09-01 01:06:06			http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=jd_site&cssurl=11f98bb8fbd2243e3ca35737f1e0f6a48e185ecc3a7a632b0068117914a75fbf502498d51c8@@	40	121	25	91	229	2031705061	河北省	张家口市	140901.00583063.35308996	啊啊	2	130728	1989-03-02	25	0	河北省	张家口市	怀安县	1	4.1703859705.927002318.1315651126.1975871234	2344690	121	25	91	229	2031705061	20142014-09-01	2014-09-01 01:03:10	1	0	征途2经典版	13	百度	网页专区标题"),
						new Text("419756653	2014-09-01 01:00:00"));
		reduceDriver
				.withOutput(
						new Text(
								"420757036	yns2014b	2014	2014-09-01	2014-09-01 01:02:56			http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=jd_site&cssurl=11f98bb8fbd2243e3ca35737f1e0f6a48e185ecc3a7a632b0068117914a75fbf502498d51c8@@	40	121	25	91	229	2031705061	河北省	张家口市	140901.00583063.35308996	啊啊	2	130728	1989-03-02	25	0	河北省	张家口市	怀安县	1	4.1703859705.927002318.1315651126.1975871234	2344690	121	25	91	229	2031705061	20142014-09-01	2014-09-01 01:01:23	1	0	征途2经典版	13	百度	网页专区标题"),
						new Text("419756653	2014-09-01 01:00:00"));
		reduceDriver
				.withOutput(
						new Text(
								"419756653	yns2014a	2014	2014-09-01	2014-09-01 01:00:00		http://act.jd.ztgame.com/jd/index.html?ad=2344690	http://reg.ztgame.com/registe/embed/fast_reg.jsp?source=jd_site&cssurl=11f98bb8fbd2243e3ca35737f1e0f6a48e185ecc3a7a632b0068117914a75fbf502498d51c8@@	40	121	25	91	229	2031705061	河北省	张家口市	140901.00583063.35308996	啊啊	2	130728	1989-03-02	25	0	河北省	张家口市	怀安县	1	4.1703859705.927002318.1315651126.1975871234	2344690	121	25  91	229	2031705061	2014	2014-09-01	2014-09-01 00:58:30	0	0	征途2经典版	13	百度	网页专区标题"),
						new Text("419756653	2014-09-01 01:00:00"));
		reduceDriver.runTest();
	}

}
