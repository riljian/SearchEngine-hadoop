/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.movehdfstohbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author ril
 */
public class MoveHdfsToHbase {

    private static final Configuration conf = HBaseConfiguration.create();
    private static final String TABLE_NAME = "page";
    private static Connection connection;


    public static void main(String[] args) throws IOException {

        
        conf.addResource(new Path("/home/course/hbase/conf/hbase-site.xml"));
        conf.addResource(new Path("/home/course/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/home/course/hadoop/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/home/course/hadoop/etc/hadoop/yarn-site.xml"));
        conf.addResource(new Path("/home/course/hadoop/etc/hadoop/mapred-site.xml"));
        
        connection = ConnectionFactory.createConnection(conf);

        createTable();

        putData();

    }

    private static void createTable() throws IOException {
        
        Admin admin = connection.getAdmin();
        
        String[] families = {"url", "title", "body"};
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        for (String family : families) {
            table.addFamily(new HColumnDescriptor(family));
        }

        if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
            admin.disableTable(TableName.valueOf(TABLE_NAME));
            admin.deleteTable(TableName.valueOf(TABLE_NAME));
        }
        
        admin.createTable(table);
    }

    private static void putData() throws IOException {
        List<Put> putList = new LinkedList();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path("/page"));
        int counter = 0;
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        for (FileStatus f : status) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(f.getPath())));
            Put put = new Put(Bytes.toBytes("row" + (++counter)));
            put.addColumn(Bytes.toBytes("url"), null, Bytes.toBytes(br.readLine())); // url
            put.addColumn(Bytes.toBytes("title"), null, Bytes.toBytes(br.readLine())); // title
            put.addColumn(Bytes.toBytes("body"), null, Bytes.toBytes(br.readLine())); // body
            putList.add(put);
        }

        table.put(putList);
        
        table.close();

    }
}
