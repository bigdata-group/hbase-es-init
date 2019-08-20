package com.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Hbase Client
 */
public class HbaseClient {

    private static Configuration configuration;


    /**
     * get Configuration instance
     */
    public static synchronized Configuration getConfiguration(String quorum,String clientPort) {
        if (configuration == null) {
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", quorum);
            configuration.set("hbase.zookeeper.property.clientPort", clientPort);
        }
        return configuration;
    }

}
