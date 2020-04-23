package com.hxqh.task.source;

import com.hxqh.domain.AssetType;
import com.hxqh.utils.JdbcUtil4Db2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by Ocean lin on 2020/3/8.
 *
 * @author Ocean lin
 */
public class AssetSoure extends RichSourceFunction<AssetType> {


    //状态位
    private volatile boolean isRunning = true;

//    private String query = "select  ASSETNUM, ASSETYPE, PRODUCTMODEL,PARENT,LOCATION from ASSET";
    private String query = "select ASSETNUM,ASSETYPE,PRODUCTMODEL,PARENT,LOCATION,PRODUCTMODELB,PRODUCTMODELC,FRACTIONRATIO,LOADRATE from ASSET";

    private AssetType assetType = new AssetType();


    @Override
    public void run(SourceContext<AssetType> ctx) throws Exception {
        //定时读取数据库的flow表，生成FLow数据
        while (isRunning) {

            Connection conn = null;

            Statement stmt = null;

            ResultSet rs = null;

            try {
                conn = JdbcUtil4Db2.getConnection();
                stmt = conn.createStatement();
                rs = stmt.executeQuery(query);

                while (rs.next()) {
                    assetType.setAssetnum(rs.getString("ASSETNUM"));
                    assetType.setAssetYpe(rs.getString("ASSETYPE"));
                    assetType.setProductModel(rs.getString("PRODUCTMODEL"));
                    assetType.setParent(rs.getString("PARENT"));
                    assetType.setLocation(rs.getString("LOCATION"));
                    assetType.setProductModelB(rs.getString("PRODUCTMODELB"));
                    assetType.setProductModelC(rs.getString("PRODUCTMODELC"));

                    assetType.setFractionRatio(Double.parseDouble(rs.getString("FRACTIONRATIO")));
                    assetType.setLoadRate(Double.parseDouble(rs.getString("LOADRATE")));

                    ctx.collect(assetType);
                }
            } finally {
                JdbcUtil4Db2.close(rs, stmt, conn);
            }
            //隔一段时间读取，可以使用更新的配置生效
            Thread.sleep(24 * 60 * 60 * 1000L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}
