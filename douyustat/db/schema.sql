create database spider character set utf8 collate utf8_general_ci;
use spider;

create table live (
  date DATE,
  hour INT(10),
  name VARCHAR(255),
  audience_total INT(32),
  live_num_total INT(32)
)default charset=utf8;