package com.decre.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

/**
 * @author Decre
 * @date 2019-5-28 22:48
 * @since 1.0.0
 * Descirption: hbase操作工具类
 */
public class HBaseTestUtils {

    public static Connection conn = null;

    String hbhost = "192.168.0.125";

    private static HBaseTestUtils instance = null;

    /**
     * 饿汉式的单例模式，没有线程安全问题
     *
     * @return
     */
    public static synchronized HBaseTestUtils getInstance() {
        return Optional.ofNullable(instance).isPresent() ? instance : new HBaseTestUtils();
    }

    private HBaseTestUtils() {
        Configuration configuration = new Configuration();

        // hbase服务地址
        configuration.set("hbase.zookeeper.quorum", hbhost);
        // zk服务端口号
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        try {
            conn = Optional.ofNullable(conn).orElse(ConnectionFactory.createConnection(configuration));
            Admin admin = getAdmin();

            String tname1 = "t_target";
            if (!admin.tableExists(TableName.valueOf(tname1))) {
                TableName tableName = TableName.valueOf(tname1);
                // 设置过期时间
                final int expireTime = 60 * 60 * 24 * 2;
                // 表描述类构造器
                TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);

                // 列族集合（每个cell值均会存储一个列族名称，所以列名称应尽量简短，避免占用过多的存储空间）
                String[] columnFamilies = {"t"};

                Arrays.stream(columnFamilies).forEach(s -> {
                    // 列族描述器构造器
                    ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(s)).setTimeToLive(expireTime).setBloomFilterType(BloomType.ROW);

                    // 获得列描述器
                    ColumnFamilyDescriptor cfd = cdb.build();
                    tdb.setColumnFamily(cfd);
                });
                // 得到表描述器
                TableDescriptor td = tdb.build();
                // 根据rowkey特征设置预分区
                byte[][] splitKeys = new byte[][]{Bytes.toBytes("1000000000"), Bytes.toBytes("2000000000"), Bytes.toBytes("3000000000"), Bytes.toBytes("4000000000")
                        , Bytes.toBytes("5000000000"), Bytes.toBytes("6000000000"), Bytes.toBytes("7000000000"), Bytes.toBytes("8000000000"), Bytes.toBytes("9000000000")};
                System.out.println(tname1 + "表创建成功！");
            } else {
                System.out.println(tname1 + "表已存在！");
            }
            // 建完表关闭admin
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 获取hbase表信息
     *
     * @param tableName
     * @return
     */
    public Table getTable(String tableName) {
        Table table = null;
        try {
            TableName tname = TableName.valueOf(tableName);
            table = (Table) conn.getTable(tname);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;
    }

    /**
     * 获取用户信息
     *
     * @return
     */
    public Admin getAdmin() {
        Admin admin = null;
        try {
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return admin;
    }

}
