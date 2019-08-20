package com.bigdata.mongodb;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoDBOutputFormat<K extends MongoDBWritable, V extends MongoDBWritable> extends OutputFormat<K, V> {
    private static Logger LOG = Logger.getLogger(MongoDBOutputFormat.class);

    /**
     * A RecordWriter that writes the reduce output to a MongoDB collection
     * 
     * @param <K>
     * @param <T>
     */
    public static class MongoDBRecordWriter<K extends MongoDBWritable, V extends MongoDBWritable> extends RecordWriter<K, V> {
        private Mongo mongo;
        private String databaseName;
        private String collectionName;
        private MongoDBConfiguration dbConf;
        private DBCollection dbCollection;
        private DBObject dbObject;
        private boolean enableFetchMethod;

        public MongoDBRecordWriter(MongoDBConfiguration dbConf, Mongo mongo, String databaseName, String collectionName) {
            this.mongo = mongo;
            this.databaseName = databaseName;
            this.collectionName = collectionName;
            this.dbConf = dbConf;
            this.enableFetchMethod = this.dbConf.isEnableUseFetchMethod();
            getDbCollection();// 创建连接
        }

        protected DBCollection getDbCollection() {
            if (null == this.dbCollection) {
                DB db = this.mongo.getDB(this.databaseName);
                if (this.dbConf.isEnableAuth()) {
                    String username = this.dbConf.getUsername();
                    String password = this.dbConf.getPassword();
//                    if (!db.authenticate(username, password.toCharArray())) {
//                        throw new RuntimeException("authenticate failure, the username:" + username + ", pwd:" + password);
//                    }
                }
                this.dbCollection = db.getCollection(this.collectionName);
            }
            return this.dbCollection;
        }

        @Override
        public void write(K key, V value) throws IOException, InterruptedException {
            if (this.enableFetchMethod) {
                this.dbObject = key.fetchWriteDBObject(null);
                this.dbObject = value.fetchWriteDBObject(this.dbObject);
                // 写数据
                this.dbCollection.insert(this.dbObject);// 在这里可以做一个缓存，一起提交，如果数据量大的情况下。
                this.dbObject = null;
            } else {
                // 直接调用写方法
                key.write(dbCollection);
                value.write(dbCollection);
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (this.mongo != null) {
                this.dbCollection = null;
                this.mongo.close();
            }
        }
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            MongoDBConfiguration dbConf = new MongoDBConfiguration(context.getConfiguration());
            String databaseName = dbConf.getOutputDatabaseName();
            String collectionName = dbConf.getOutputCollectionName();
            Mongo mongo = dbConf.getMongoConnection();
            return new MongoDBRecordWriter<K, V>(dbConf, mongo, databaseName, collectionName);
        } catch (Exception e) {
            LOG.error("Create the record writer occur exception.", e);
            throw new IOException(e);
        }
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        // 不进行检测
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        // 由于outputcommitter主要作用是提交jar，分配jar的功能。所以我们这里直接使用FileOutputCommitter
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
    }

    /**
     * 设置output属性
     * 
     * @param job
     * @param databaseName
     * @param collectionName
     */
    public static void setOutput(Job job, String databaseName, String collectionName) {
        job.setOutputFormatClass(MongoDBOutputFormat.class);
        job.setReduceSpeculativeExecution(false);
        MongoDBConfiguration mdc = new MongoDBConfiguration(job.getConfiguration());
        mdc.setOutputCollectionName(collectionName);
        mdc.setOutputDatabaseName(databaseName);
    }

    /**
     * 静止使用fetch方法
     * 
     * @param conf
     */
    public static void disableFetchMethod(Configuration conf) {
        conf.setBoolean(MongoDBConfiguration.OUTPUT_USE_FETCH_METHOD_PROPERTY, false);
    }
}
