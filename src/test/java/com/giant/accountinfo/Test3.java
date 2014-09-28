package com.giant.accountinfo;

import com.google.common.base.Joiner;


public class Test3 {

	public static void main(String[] args) {
		String line = "351758709	svvv0171	2014	2014-09-01	2014-09-01 00:00:02			http://reg.ztgame.com/registe/embed/fast_reg.jsp	0	111	77	127	167	1867349927	江西省	九江市		冯水婕	1	420902	1952-01-10	62	1	湖北省	孝感市	市辖区-孝南区	1	2.13440958.2083830492.2003945581.1759555151	0	0	0	0	0	0	0000	0000-00-00	0000-00-00 00:00:00	0	0		0		";
		String[] cols = line.split("	");
		for(String col:cols){
			System.out.println(col);
		}
		
		if (cols.length < 39) {
			return;
		}
		
		String regtime = cols[4];
		String key4="col4+"+regtime.substring(0,13);
		System.out.println(key4);
		
		String preurl = cols[6];
		String key6="col6+"+preurl;
		System.out.println(key6);
		
		String cururl = cols[7];
		String key7="col7+"+cururl;
		System.out.println(key7);
		
		String reggametype= cols[8];
		String key8="col8+"+reggametype;
		System.out.println(key8);
		
		String regnip= cols[13];
		String key13="col13+"+regnip;
		System.out.println(key13);
		
		String regpro= cols[14];
		String key14="col14+"+regpro;
		System.out.println(key14);
		
		String regcity= cols[15];
		String key15="col15+"+regcity;
		System.out.println(key15);
		
		String cookieid= cols[16];
		String key16="col16+"+cookieid;
		System.out.println(key16);
		
		String sfzname= cols[17];
		String key17="col17+"+sfzname;
		System.out.println(key17);
		
		String regage= cols[21];
		String key21="col21+"+regage;
		System.out.println(key21);
		
		String sfzmd5= cols[27];
		String key27="col27+"+sfzmd5;
		System.out.println(key27);
		
		String adid= cols[28];
		String key28="col28+"+adid;
		System.out.println(key28);
		
		
		String newkey = Joiner.on("+").join(regnip, cookieid, regtime.substring(11, 13), sfzmd5, adid);
//		context.write(new Text(newkey), value);

	}

}
