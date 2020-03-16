create table RE_COMPARE_QUARTER
(
    RECOMPAREQUARTERID BIGINT
        primary key,
    IEDNAME            VARGRAPHIC(100),
    ASSETYPE           VARGRAPHIC(100),
    SCORE              DECIMAL(8, 4),
    CREATETIME         VARGRAPHIC(100),
    COMPARISON         VARGRAPHIC(50),
    RATIO              DECIMAL(8, 4),
    ROWSTAMP           BIGINT not null
);

comment on table RE_COMPARE_QUARTER is '季度对比及变化率表';

comment on column RE_COMPARE_QUARTER.IEDNAME is '设备编码';

comment on column RE_COMPARE_QUARTER.ASSETYPE is '设备类别';

comment on column RE_COMPARE_QUARTER.SCORE is '得分';

comment on column RE_COMPARE_QUARTER.CREATETIME is '时间';

comment on column RE_COMPARE_QUARTER.COMPARISON is '对比上季度';

comment on column RE_COMPARE_QUARTER.RATIO is '变化率';

INSERT INTO MAXIMO.RE_COMPARE_QUARTER (RECOMPAREQUARTERID, IEDNAME, ASSETYPE, SCORE, CREATETIME, COMPARISON, RATIO, ROWSTAMP) VALUES (1, 'AH01', '中压开关设备', 50.0000, '2020-1', '大致持平（分数相差±5%内）', -0.0384, 3707048);
INSERT INTO MAXIMO.RE_COMPARE_QUARTER (RECOMPAREQUARTERID, IEDNAME, ASSETYPE, SCORE, CREATETIME, COMPARISON, RATIO, ROWSTAMP) VALUES (2, 'AH01', '中压开关设备', 50.0000, '2020-1', '有所提升', 0.1363, 3707049);