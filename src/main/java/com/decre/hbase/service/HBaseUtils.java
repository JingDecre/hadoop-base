package com.decre.hbase.service;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/**
 * @author Decre
 * @date 2019-5-28 23:25
 * @since 1.0.0
 * Descirption:
 */
public abstract class HBaseUtils {
    public abstract void getAllTables() throws Exception;

    public abstract void createTable(String tableName, String[] family) throws Exception;

    public abstract void createTable(HTableDescriptor htds) throws Exception;

    public abstract void createTable(String tableName, HTableDescriptor htds) throws Exception;

    public abstract void descTable(String tableName) throws Exception;

    public abstract boolean existTable(String tableName) throws Exception;

    public abstract void disableTable(String tableName) throws Exception;

    public abstract void dropTable(String tableName) throws Exception;

    public abstract void modifyTable(String tableName) throws Exception;

    public abstract void modifyTable(String tableName, String[] addColumn, String[] removeColumn) throws Exception;

    public abstract void modifyTable(String tableName, HColumnDescriptor hcds) throws Exception;

    public abstract void putData(String tableName, String rowKey, String familyName, String columnName, String value)
            throws Exception;

    public abstract void putData(String tableName, String rowKey, String familyName, String columnName, String value,
                                 long timestamp) throws Exception;

    public abstract Result getResult(String tableName, String rowKey) throws Exception;

    public abstract Result getResult(String tableName, String rowKey, String familyName) throws Exception;

    public abstract Result getResult(String tableName, String rowKey, String familyName, String columnName) throws Exception;

    public abstract Result getResultByVersion(String tableName, String rowKey, String familyName, String columnName,
                                              int versions) throws Exception;

    public abstract ResultScanner getResultScann(String tableName) throws Exception;

    public abstract ResultScanner getResultScann(String tableName, Scan scan) throws Exception;

    public abstract void deleteColumn(String tableName, String rowKey) throws Exception;

    public abstract void deleteColumn(String tableName, String rowKey, String falilyName) throws Exception;

    public abstract void deleteColumn(String tableName, String rowKey, String falilyName, String columnName) throws Exception;
}
