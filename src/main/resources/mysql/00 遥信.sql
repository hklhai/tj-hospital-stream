CREATE TABLE YX_CURRENT
(
    YXID         int NOT NULL AUTO_INCREMENT,
    IEDNAME      varchar(100) DEFAULT NULL,
    COLTIME      datetime     DEFAULT NULL,
    VARIABLENAME varchar(100) DEFAULT NULL,
    VAL          int,
    CREATETIME   datetime     DEFAULT NULL,
    PRIMARY KEY (`YXID`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;



CREATE TABLE YX_SCORE
(
    YXSCOREID    int NOT NULL AUTO_INCREMENT,
    IEDNAME      varchar(100) DEFAULT NULL,
    COLTIME      datetime     DEFAULT NULL,
    SCORE        int,
    HIGHLEVEL    int,
    VARIABLENAME varchar(100) DEFAULT NULL,
    VAL          int,
    CREATETIME   datetime     DEFAULT NULL,
    PRIMARY KEY (`YXSCOREID`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

