create table RE_COMPARE_YEAR
(
    RECOMPAREYEARID BIGINT
        primary key,
    IEDNAME         VARGRAPHIC(100),
    ASSETYPE        VARGRAPHIC(100),
    SCORE           DECIMAL(8, 4),
    CREATETIME      VARGRAPHIC(100),
    COMPARISON      VARGRAPHIC(50),
    RATIO           DECIMAL(8, 4),
    ROWSTAMP        BIGINT not null
);

comment on table RE_COMPARE_YEAR is '年度对比及变化率表';

comment on column RE_COMPARE_YEAR.RECOMPAREYEARID is '主键';

comment on column RE_COMPARE_YEAR.IEDNAME is '设备编码';

comment on column RE_COMPARE_YEAR.ASSETYPE is '设备类别';

comment on column RE_COMPARE_YEAR.SCORE is '得分';

comment on column RE_COMPARE_YEAR.CREATETIME is '年份时间';

comment on column RE_COMPARE_YEAR.COMPARISON is '对比结果';

comment on column RE_COMPARE_YEAR.RATIO is '变化率';

INSERT INTO MAXIMO.RE_COMPARE_YEAR (RECOMPAREYEARID, IEDNAME, ASSETYPE, SCORE, CREATETIME, COMPARISON, RATIO, ROWSTAMP) VALUES (1, 'AH01', '中压开关设备', 50.0000, '2020', '有所提升', 0.6666, 3707906);