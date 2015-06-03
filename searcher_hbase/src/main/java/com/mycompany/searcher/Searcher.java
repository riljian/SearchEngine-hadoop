/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.searcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author ril
 */
public class Searcher {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("/home/course/hbase/conf/hbase-site.xml"));
        conf.addResource(new Path("/home/course/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/home/course/hadoop/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path("/home/course/hadoop/etc/hadoop/yarn-site.xml"));
        conf.addResource(new Path("/home/course/hadoop/etc/hadoop/mapred-site.xml"));
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("page"));

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("url"), null);
        scan.addColumn(Bytes.toBytes("title"), null);
        scan.addColumn(Bytes.toBytes("body"), null);

        ResultScanner scanner = table.getScanner(scan);

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
        System.out.print("Please input a keyword:\t");
        String keyword = in.readLine();

        boolean first = true;
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            String body = Bytes.toString(result.getValue(Bytes.toBytes("body"), null));
            int index = body.indexOf(keyword), counter = 0;
            while (index < body.length() && index != -1) {
                ++counter;
                index = body.indexOf(keyword, index + keyword.length());
            }
            if (counter > 0) {
                if (first) {
                    first = false;
                    System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("body"), null)));
                }
                System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("url"), null)));
                System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("title"), null)) + ":" + counter);
            }
        }

        scanner.close();
    }
}
