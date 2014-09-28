package com.giant.accountinfo.rank;

import com.google.common.base.Strings;

public class RankFunction {

	public static double getAllRank(double xs[]) {
		double ks[] = { 0.2, 0, 0.4, 0.2, 0.9, 0.9, 0, 0.9 };
		// 动态调整字段的权值
		if (xs[1] > 0)
			ks[1] = 0.2;
		if (xs[6] > 0.1)
			ks[6] = 0.5;
		return getAllRank(xs, ks);
	}

	private static double getAllRank(double xs[], double ks[]) {
		double xksum = 0;
		double ksum = 0;
		for (int i = 0; i < xs.length; i++) {
			xksum = xksum + xs[i] * ks[i];
			ksum = ksum + ks[i];
		}
		return xksum / ksum;
	}

	/**
	 * 同一小时注册的账户数
	 * 
	 * <pre>
	 * 使用指数函数计算
	 * 
	 * 函数参数： 底值=0.677 
	 * 公式: y=1-(2/3)^x  
	 * 计算结果权值：0.1  --> 考虑到会存在注册峰值
	 * </pre>
	 * 
	 * @param regtime
	 * @param times
	 * @return
	 */
	public static double getRegtimeRank(String regtime, int times, int allrows) {
		if (times == 1)
			return 0;
		return 1 - Math.pow((2.0 / 3.0), times);
	}

	/**
	 * 注册之前的页面
	 * 
	 * <pre>
	 * 如果注册之前的页面为空，给定制 0.5
	 *  
	 * 计算结果权值：0.1
	 * </pre>
	 * 
	 * @param regtime
	 * @param times
	 * @return
	 */
	public static double getPreurlRank(String preurl, int times, int allrows) {
		if (times == 1)
			return 0;
		double y = 0.0;
		if (Strings.isNullOrEmpty(preurl)) {
			y = 0.7;
		}
		return y;
	}

	/**
	 * 当前注册页面
	 * 
	 * <pre>
	 * 使用指数函数计算
	 * 
	 * 函数参数： 底值=0.677 
	 * 公式: y=1-(2/3)^x  
	 * 计算结果权值：0.1  --> 考虑到会存在注册峰值
	 * </pre>
	 * 
	 * @param regtime
	 * @param times
	 * @return
	 */
	public static double getCurlurlRank(String cururl, int times, int allrows) {
		if (times == 1)
			return 0;
		double y = 0.0;
		if (Strings.isNullOrEmpty(cururl))
			y = 0.9;
		else if (1.0 * times / allrows > 0.8)
			y = 0.5;
		else
			y = 1 - Math.pow((9.0 / 10.0), times);
		return y;
	}

	/**
	 * 注册的游戏类型
	 * 
	 * <pre>
	 * 使用指数函数计算
	 * 
	 * 函数参数： 底值=0.677 
	 * 公式: y=1-(2/3)^x  
	 * 计算结果权值：0.1  --> 考虑到会存在注册峰值
	 * </pre>
	 * 
	 * @param regtime
	 * @param times
	 * @return
	 */
	public static double getReggametypeRank(String reggametype, int times, int allrows) {
		if (times == 1)
			return 0;
		double y = 0.0;
		if (Strings.isNullOrEmpty(reggametype) || reggametype.trim().equals("0")) {
			y = 0.9;
		} else
			y = 1 - Math.pow((99.0 / 100.0), times);
		return y;
	}

	/**
	 * 注册的Ip
	 * 
	 * <pre>
	 * 给予较大的权值，主要判断条件
	 * 
	 * 计算结果权值：0.8
	 * </pre>
	 * 
	 * @param regtime
	 * @param times
	 * @return
	 */
	public static double getRegnipRank(String regnip, int times, int allrows) {
		if (times == 1)
			return 0;
		double y = 0.0;
		if (Strings.isNullOrEmpty(regnip) || regnip.trim().equals("0")) {
			y = 0.8;
		} else
			y = 1 - Math.pow((1 / 2), times);
		return y;
	}

	/**
	 * 注册的浏览器cookieid
	 * 
	 * <pre>
	 * 给予较大的权值，主要判断条件
	 * 
	 * 计算结果权值：0.8
	 * </pre>
	 * 
	 * @param regtime
	 * @param times
	 * @return
	 */
	public static double getCookieidRank(String cookieid, int times, int allrows) {
		if (times == 1)
			return 0;
		double y = 0.0;
		if (Strings.isNullOrEmpty(cookieid) || cookieid.trim().equals("0")) {
			y = 0.8;
		} else
			y = 1 - Math.pow((1 / 2), times);
		return y;
	}

	/**
	 * 注册的Ip
	 * 
	 * <pre>
	 * 给予较大的权值，主要判断条件
	 * 
	 * 计算结果权值：0.8
	 * </pre>
	 * 
	 * @param regtime
	 * @param times
	 * @return
	 */
	public static double getRegageRank(String regage, int times, int allrows) {
		if (times == 1)
			return 0;
		double y = 0.0;
		int age = Integer.parseInt(regage);
		if (age > 80)
			y = 0.99;
		else if (age > 70)
			y = 0.9;
		else if (age > 60)
			y = 0.8;
		else if (age > 50)
			y = 0.7;
		else if (age > 40)
			y = 0.1;
		else if (age > 18)
			y = 0;
		else if (age > 0)
			y = 0.1;
		else
			y = 0.99;
		return y;
	}

	/**
	 * 身份证md5
	 * 
	 * <pre>
	 * 给予较大的权值，主要判断条件
	 * 
	 * 计算结果权值：0.8
	 * </pre>
	 * 
	 * @param regtime
	 * @param times
	 * @return
	 */
	public static double getSfzmd5Rank(String sfzmd5, int times, int allrows) {
		if (times == 1)
			return 0;
		double y = 0.0;
		if (Strings.isNullOrEmpty(sfzmd5) || sfzmd5.trim().equals("0")) {
			y = 0.8;
		} else
			y = 1 - Math.pow((1 / 3), times);
		return y;
	}
}
