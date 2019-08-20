package com.bigdata.hadoop;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import com.bigdata.hadoop.mapper.ESInitMapper;
import com.bigdata.hbase.HbaseClient;
import com.bigdata.utils.PropertiesUtil;

/**
 * Call Es init job
 */
public class ESInitCall {

    public static void main(String args[]) throws Exception {
        String index = args[0];
        String tableName = args[1];
        String columnFamily = args[2];
        if (StringUtils.isBlank(index) || StringUtils.isBlank(tableName) || StringUtils.isBlank(columnFamily)) {
            System.err.println("please input ES index or hbase tableName or hbase table's columnFamily as three parameters");
            System.exit(2);
        }
        System.out.println("target index:" + index);
        System.out.println("target table:" + tableName);
        System.out.println("target columnFamily:" + columnFamily);
        System.out.println("es.cluster.name:" + PropertiesUtil.getStringByKey("es.cluster.name"));
        System.out.println("es.cluster.port:" + PropertiesUtil.getStringByKey("es.cluster.port"));
        System.out.println("es.cluster.hosts:" + PropertiesUtil.getStringByKey("es.cluster.hosts"));
        System.out.println("hbase.zookeeper.quorum:" + PropertiesUtil.getStringByKey("hbase.zookeeper.quorum"));
        System.out.println("hbase.zookeeper.property.clientPort:"+ PropertiesUtil.getStringByKey("hbase.zookeeper.property.clientPort"));
        Configuration conf = HbaseClient.getConfiguration(PropertiesUtil.getStringByKey("hbase.zookeeper.quorum"), PropertiesUtil.getStringByKey("hbase.zookeeper.property.clientPort"));
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("index", index);
        conf.set("tableName", tableName);
        conf.set("columnFamily", columnFamily);
        conf.set("fields",PropertiesUtil.getStringByKey(tableName + "_fields"));
        conf.set("es.cluster.name", PropertiesUtil.getStringByKey("es.cluster.name"));
        conf.set("es.cluster.port", PropertiesUtil.getStringByKey("es.cluster.port"));
        conf.set("es.cluster.hosts", PropertiesUtil.getStringByKey("es.cluster.hosts"));
        Job job = Job.getInstance(conf, "ESInit " + index + " (" + tableName+ ") Call Job");
        job.setJarByClass(ESInitCall.class);
        
        Scan scan = new Scan();
        scan.setCaching(500); // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false); // don't set to true for MR jobs
        // 设置map过程
        TableMapReduceUtil.initTableMapperJob(
                tableName,      // input table
                scan,              // Scan instance to control CF and attribute selection
                ESInitMapper.class,   // mapper class
                null,              // mapper output key
                null,              // mapper output value
                job);
        TableMapReduceUtil.initTableReducerJob(
                tableName,      // output table
                null,             // reducer class
                job);
        job.setNumReduceTasks(0);
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }
}
