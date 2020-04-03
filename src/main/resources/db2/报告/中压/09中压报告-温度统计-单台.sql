CREATE TABLE MAXIMO.RE_VOLTAGE_TEMPERATURE
(
    REVOLTAGETEMPERATUREID  BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1 ),
    IEDName                 VARGRAPHIC(100 CODEUNITS16),
    AUpperArmTemperatureAvg DECIMAL(31, 4),
    BUpperArmTemperatureAvg DECIMAL(31, 4),
    CUpperArmTemperatureAvg DECIMAL(31, 4),
    AUpperArmTemperatureMax DECIMAL(31, 4),
    BUpperArmTemperatureMax DECIMAL(31, 4),
    CUpperArmTemperatureMax DECIMAL(31, 4),
    ALowerArmTemperatureAvg DECIMAL(31, 4),
    BLowerArmTemperatureAvg DECIMAL(31, 4),
    CLowerArmTemperatureAvg DECIMAL(31, 4),
    ALowerArmTemperatureMax DECIMAL(31, 4),
    BLowerArmTemperatureMax DECIMAL(31, 4),
    CLowerArmTemperatureMax DECIMAL(31, 4),
    ACableTemperatureAvg    DECIMAL(31, 4),
    BCableTemperatureAvg    DECIMAL(31, 4),
    CCableTemperatureAvg    DECIMAL(31, 4),
    ACableTemperatureMax    DECIMAL(31, 4),
    BCableTemperatureMax    DECIMAL(31, 4),
    CCableTemperatureMax    DECIMAL(31, 4),
    TIMEPOINT               VARGRAPHIC(50 CODEUNITS16),
    CREATETIME              TIMESTAMP,
    PRIMARY KEY (REVOLTAGETEMPERATUREID)
);


