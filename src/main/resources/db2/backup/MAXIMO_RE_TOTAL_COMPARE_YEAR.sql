create table RE_TOTAL_COMPARE_YEAR
(
    RETOTALCOMPAREYEARID BIGINT
        primary key,
    ASSETYPE             VARGRAPHIC(100),
    PRODUCTMODEL         VARGRAPHIC(100),
    LOCATION             VARGRAPHIC(100),
    SCORE                DECIMAL(8, 4),
    CREATETIME           VARGRAPHIC(100),
    COMPARISON           VARGRAPHIC(50),
    RATIO                DECIMAL(8, 4)
);

comment on table RE_TOTAL_COMPARE_YEAR is '总体年度对比';

comment on column RE_TOTAL_COMPARE_YEAR.RETOTALCOMPAREYEARID is '主键';

comment on column RE_TOTAL_COMPARE_YEAR.ASSETYPE is '设备类别';

comment on column RE_TOTAL_COMPARE_YEAR.PRODUCTMODEL is '设备型号';

comment on column RE_TOTAL_COMPARE_YEAR.LOCATION is '位置';

comment on column RE_TOTAL_COMPARE_YEAR.SCORE is '评分';

comment on column RE_TOTAL_COMPARE_YEAR.CREATETIME is '时间-年度';

comment on column RE_TOTAL_COMPARE_YEAR.COMPARISON is '对比结果';

comment on column RE_TOTAL_COMPARE_YEAR.RATIO is '变化率';

INSERT INTO MAXIMO.RE_TOTAL_COMPARE_YEAR (RETOTALCOMPAREYEARID, ASSETYPE, PRODUCTMODEL, LOCATION, SCORE, CREATETIME, COMPARISON, RATIO) VALUES (1, '中压开关设备', '35kV', 'A001', 3.5000, '2019', '', 0.0000);
INSERT INTO MAXIMO.RE_TOTAL_COMPARE_YEAR (RETOTALCOMPAREYEARID, ASSETYPE, PRODUCTMODEL, LOCATION, SCORE, CREATETIME, COMPARISON, RATIO) VALUES (2, '中压开关设备', '35kV', 'A001', 4.7377, '2020', '有所提升', 0.3536);