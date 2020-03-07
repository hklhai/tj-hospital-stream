package com.hxqh.batch.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.hxqh.domain.Flow;
import com.hxqh.utils.Md5Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * HBase同步操作业务
 * <p>
 * Created by Ocean lin on 2020/3/7.
 *
 * @author Ocean lin
 */
@Slf4j
public class HbaseSyncService implements Serializable {
    /**
     * HBase操作模板
     */
    private HbaseTemplate hbaseTemplate;

    public HbaseSyncService(HbaseTemplate hbaseTemplate) {
        this.hbaseTemplate = hbaseTemplate;
    }

    public void sync(Flow flow, FlatMessage dml) {
        if (flow != null) {
            String type = dml.getType();
            if (type != null && type.equalsIgnoreCase("INSERT")) {
                insert(flow, dml);
            } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
//                update(flow, dml);
            } else if (type != null && type.equalsIgnoreCase("DELETE")) {
//                delete(flow, dml);
            }
            if (log.isDebugEnabled()) {
                log.debug("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
            }
        }
    }


    /**
     * 插入操作
     *
     * @param flow 配置项
     * @param dml  DML数据
     */
    private void insert(Flow flow, FlatMessage dml) {
        List<Map<String, String>> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        int i = 1;
        boolean complete = false;
        List<HRow> rows = new ArrayList<>();
        for (Map<String, String> r : data) {
            HRow hRow = new HRow();

            // 拼接复合rowKey
            if (flow.getRowKey() != null) {
                String[] rowKeyColumns = flow.getRowKey().trim().split(",");
                String rowKeyVale = getRowKey(rowKeyColumns, r);
                hRow.setRowKey(Bytes.toBytes(rowKeyVale));
            }

            convertData2Row(flow, hRow, r);

            if (hRow.getRowKey() == null) {
                throw new RuntimeException("empty rowKey: " + hRow.toString() + ",Flow: " + flow.toString());
            }
            rows.add(hRow);
            complete = false;

            if (i % flow.getCommitBatch() == 0 && !rows.isEmpty()) {
                hbaseTemplate.puts(flow.getHbaseTable(), rows);
                rows.clear();
                complete = true;
            }
            i++;
        }
        if (!complete && !rows.isEmpty()) {
            hbaseTemplate.puts(flow.getHbaseTable(), rows);
        }

    }

    /**
     * 获取复合字段作为rowKey的拼接
     *
     * @param rowKeyColumns 复合rowK对应的字段
     * @param data          数据
     * @return
     */
    private static String getRowKey(String[] rowKeyColumns, Map<String, String> data) {
        StringBuilder rowKeyValue = new StringBuilder();
        for (String rowKeyColumnName : rowKeyColumns) {
            Object obj = data.get(rowKeyColumnName);
            if (obj != null) {
                rowKeyValue.append(obj.toString());
            }
            rowKeyValue.append("|");
        }
        int len = rowKeyValue.length();
        if (len > 0) {
            rowKeyValue.delete(len - 1, len);
        }

        //可自行扩展支持多种rowkey生成策略，这里写死为md5前缀
        return Md5Utils.getMD5String(rowKeyValue.toString()).substring(0, 8) + "_" + rowKeyValue.toString();
    }


    /**
     * 将Map数据转换为HRow行数据
     *
     * @param flow hbase映射配置
     * @param hRow 行对象
     * @param data Map数据
     */
    private static void convertData2Row(Flow flow, HRow hRow, Map<String, String> data) {
        String familyName = flow.getFamily();

        for (Map.Entry<String, String> entry : data.entrySet()) {
            if (entry.getValue() != null) {

                byte[] bytes = Bytes.toBytes(entry.getValue().toString());

                String qualifier = entry.getKey();
                if (flow.isUppercaseQualifier()) {
                    qualifier = qualifier.toUpperCase();
                }

                hRow.addCell(familyName, qualifier, bytes);
            }
        }
    }
}
