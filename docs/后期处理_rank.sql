// 创建hive外部表，核对数据

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

select count(1) from tmp2 ; -- 147542

// 查看计算结果的分布
+-------+-----+------+--------+------+-----+------+-----+-----+-----+
| pr9   | pr8 | pr7  | pr6    | pr5  | pr4 | pr3  | pr2 | pr1 | pr0 |
+-------+-----+------+--------+------+-----+------+-----+-----+-----+
| 14453 | 11  | 1708 | 119202 | 2969 | 622 | 8115 | 386 | 72  | 4   |
+-------+-----+------+--------+------+-----+------+-----+-----+-----+
select sum(IF(rank > 0.9,1,0)) as PR9,
       sum(IF(rank > 0.8 and rank <=0.9,1,0)) as PR8,
       sum(IF(rank > 0.7 and rank <=0.8,1,0)) as PR7,
       sum(IF(rank > 0.6 and rank <=0.7,1,0)) as PR6,
       sum(IF(rank > 0.5 and rank <=0.6,1,0)) as PR5,
       sum(IF(rank > 0.4 and rank <=0.5,1,0)) as PR4,
       sum(IF(rank > 0.3 and rank <=0.4,1,0)) as PR3,
       sum(IF(rank > 0.2 and rank <=0.3,1,0)) as PR2,
       sum(IF(rank > 0.1 and rank <=0.2,1,0)) as PR1,
       sum(IF(rank > 0 and rank <=0.1,1,0)) as PR0
from tmp2 ;

// 验证数据
select * from tmp2 where rank>0.9 limit 10;

// 导出核对数据
drop table tmp3;
create table tmp3 as select * from tmp2 where rank > 0.9 order by regnip,sfzmd5,account;

hadoop dfs -get /user/hive/warehouse/tmp3/000000_0

// rank筛选出来，但是group by没有筛选出来的
select tmp3.* from tmp3 left outer join tmp4 on tmp3.uid=tmp4.uid where tmp4.uid is null limit 10;
select tmp4.* from tmp4 left outer join tmp3 on tmp3.uid=tmp4.uid where tmp3.uid is null limit 10;