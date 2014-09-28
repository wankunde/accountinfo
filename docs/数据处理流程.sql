hadoop jar accountinfo-1.0.0.jar com.giant.accountinfo.JobGroup
hadoop jar accountinfo-1.0.0.jar com.giant.accountinfo.rank.JobGroup

// sql 1

drop table tmp1;
CREATE external TABLE tmp1 (
  uid int,
  account string,
  regyear smallint,
  regdate string,
  regtime string,
  email string,
  preurl string,
  cururl string,
  reggametype int,
  regip1 smallint,
  regip2 smallint,
  regip3 smallint,
  regip4 smallint,
  regnip bigint,
  regpro string,
  regcity string,
  cookieid string,
  sfzname string,
  issfzname tinyint,
  sfznum6 int,
  sfzbir string,
  regage smallint,
  sfzsex tinyint,
  sfzpro string,
  sfzcity string,
  sfzxq string,
  issfznum tinyint,
  sfzmd5 string,
  adid int,
  adip1 smallint,
  adip2 smallint,
  adip3 smallint,
  adip4 smallint,
  adnip bigint,
  adyear string,
  addate string,
  adtime string,
  adhour tinyint,
  adgametype int,
  adgame string,
  mediaid smallint,
  media string,
  loc string,
  fuid int,
  fregtime string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
location '/tmp/wankun/accountgroup2/';

drop table tmp4;
create table tmp4 as select * from tmp1 where uid!=fuid order by regnip,sfzmd5,account;

// sql 2
drop table tmp2;
CREATE external TABLE tmp2 (
  uid int,
  account string,
  regyear smallint,
  regdate string,
  regtime string,
  email string,
  preurl string,
  cururl string,
  reggametype int,
  regip1 smallint,
  regip2 smallint,
  regip3 smallint,
  regip4 smallint,
  regnip bigint,
  regpro string,
  regcity string,
  cookieid string,
  sfzname string,
  issfzname tinyint,
  sfznum6 int,
  sfzbir string,
  regage smallint,
  sfzsex tinyint,
  sfzpro string,
  sfzcity string,
  sfzxq string,
  issfznum tinyint,
  sfzmd5 string,
  adid int,
  adip1 smallint,
  adip2 smallint,
  adip3 smallint,
  adip4 smallint,
  adnip bigint,
  adyear string,
  addate string,
  adtime string,
  adhour tinyint,
  adgametype int,
  adgame string,
  mediaid smallint,
  media string,
  loc string,
  rank double
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
location '/tmp/wankun/accountrank/';

drop table tmp3;
create table tmp3 as select * from tmp2 where rank > 0.9 order by regnip,sfzmd5,account;

// rank筛选出来，但是group by没有筛选出来的
select tmp3.* from tmp3 left outer join tmp4 on tmp3.uid=tmp4.uid where tmp4.uid is null limit 10;
select tmp4.* from tmp4 left outer join tmp3 on tmp3.uid=tmp4.uid where tmp3.uid is null limit 10;