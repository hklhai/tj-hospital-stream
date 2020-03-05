-- scement
create user canal@'%' identified by 'canal';
grant select on canal.* to 'canal'@'%' identified by 'canal';


grant all privileges on `canal`.* to 'canal'@'%';
grant all privileges on *.* to 'canal'@'%';


flush privileges;

create database canal default character set utf8;

use mysql;
UPDATE user SET Super_Priv='Y' WHERE user='canal';
flush privileges;


ALTER USER 'canal'@'%'   IDENTIFIED WITH mysql_native_password  BY 'canal';