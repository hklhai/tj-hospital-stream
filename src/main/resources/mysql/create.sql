-- scement
create user tj@'%' identified by 'mko09ijn*';
grant select on tj.* to 'tj'@'%' identified by 'mko09ijn*';


grant all privileges on `tj`.* to 'tj'@'%';

flush privileges;

create database tj default character set utf8;

use mysql;
UPDATE user SET Super_Priv='Y' WHERE user='tj';
flush privileges;


ALTER USER 'tj'@'%'   IDENTIFIED WITH mysql_native_password  BY 'mko09ijn*';