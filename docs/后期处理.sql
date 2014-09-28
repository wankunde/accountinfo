// 创建hive外部表，核对数据

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


select count(1) from tmp1 ; -- 147542
select count(1) from tmp1 where uid!=fuid;  -- 一次过滤 12145   两次过滤  12521



// 验证数据
select * from tmp1 where fuid=419756653 limit 10;

// 导出核对数据
drop table tmp4;
create table tmp4 as select * from tmp1 where uid!=fuid order by regnip,sfzmd5,account;

hadoop dfs -get /user/hive/warehouse/tmp4/000000_0
