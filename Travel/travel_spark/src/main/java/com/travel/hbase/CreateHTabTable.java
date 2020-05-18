package com.travel.hbase;

import com.travel.common.Constants;
import com.travel.utils.HbaseTools;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class CreateHTabTable {

    public static void main(String[] args) throws IOException {
        Connection hbaseConn = HbaseTools.getHbaseConn();
        String[] tableNames = new String[]{Constants.HTAB_HAIKOU_ORDER,Constants.HTAB_GPS};
        Admin admin = hbaseConn.getAdmin();
        for (String tableName : tableNames) {
            if(!admin.tableExists(TableName.valueOf(tableName))){
                HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
                hTableDescriptor.addFamily(new HColumnDescriptor(Constants.DEFAULT_FAMILY));
                admin.createTable(hTableDescriptor);
            }
        }
        admin.close();
        hbaseConn.close();
    }
}
