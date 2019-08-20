package com.bigdata.mongodb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongoDBInputFormat<T extends MongoDBWritable> extends InputFormat<LongWritable, T> implements Configurable {
    private static final Logger LOG = Logger.getLogger(MongoDBInputFormat.class);

    /**
     * 空的对象，主要作用是不进行任何操作，类似于NullWritable
     */
    public static class NullMongoDBWritable implements MongoDBWritable, Writable {
        @Override
        public void write(DBCollection collection) throws MongoException {
            // TODO Auto-generated method stub
        }

        @Override
        public void readFields(DBObject object) throws MongoException {
            // TODO Auto-generated method stub
        }

        @Override
        public void write(DataOutput out) throws IOException {
            // TODO Auto-generated method stub
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            // TODO Auto-generated method stub
        }

        @Override
        public DBObject fetchWriteDBObject(DBObject old) throws MongoException {
            // TODO Auto-generated method stub
            return old;
        }

    }

    /**
     * MongoDB的input split类
     */
    public static class MongoDBInputSplit extends InputSplit implements Writable {
        private long end = 0;
        private long start = 0;

        /**
         * 默认构造方法
         */
        public MongoDBInputSplit() {
        }

        /**
         * 便利的构造方法
         * 
         * @param start
         *            集合中查询的文档开始行号
         * @param end
         *            集合中查询的文档结束行号
         */
        public MongoDBInputSplit(long start, long end) {
            this.start = start;
            this.end = end;
        }

        public long getEnd() {
            return end;
        }

        public long getStart() {
            return start;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(this.start);
            out.writeLong(this.end);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.start = in.readLong();
            this.end = in.readLong();
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            // 分片大小
            return this.end - this.start;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            // TODO 返回一个空的数组，表示不进行数据本地化的优化，那么map执行节点随机选择。
            return new String[] {};
        }

    }

    protected MongoDBConfiguration mongoConfiguration; // mongo相关配置信息
    protected Mongo mongo; // mongo连接
    protected String databaseName; // 连接的数据库名称
    protected String collectionName; // 连接的集合名称
    protected DBObject conditionQuery; // 选择条件
    protected DBObject fieldQuery; // 需要的字段条件

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
        DBCollection dbCollection = null;
        try {
            dbCollection = this.getDBCollection();
            // 获取数量大小
            long count = dbCollection.count(this.getConditionQuery());
            int chunks = job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
            long chunkSize = (count / chunks); // 分片数量

            // 开始分片，只是简单的分配每个分片的数据量
            List<InputSplit> splits = new ArrayList<InputSplit>();
            for (int i = 0; i < chunks; i++) {
                MongoDBInputSplit split = null;
                if ((i + 1) == chunks) {
                    split = new MongoDBInputSplit(i * chunkSize, count);
                } else {
                    split = new MongoDBInputSplit(i * chunkSize, (i * chunkSize) + chunkSize);
                }
                splits.add(split);
            }
            return splits;
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            dbCollection = null;
            closeConnection(); // 关闭资源的连接
        }
    }

    @Override
    public RecordReader<LongWritable, T> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return createRecordReader((MongoDBInputSplit) split, context.getConfiguration());
    }

    protected RecordReader<LongWritable, T> createRecordReader(MongoDBInputSplit split, Configuration conf) {
        // 获取从mongodb中读取数据需要转换成的value class，默认为NullMongoDBWritable
        Class<? extends MongoDBWritable> valueClass = this.mongoConfiguration.getValueClass();
        return new MongoDBRecordReader<T>(split, valueClass, conf, getDBCollection(), getConditionQuery(), getFieldQuery());
    }

    @Override
    public void setConf(Configuration conf) {
        mongoConfiguration = new MongoDBConfiguration(conf);
        databaseName = this.mongoConfiguration.getInputDatabaseName(); // 输入数据的数据库
        collectionName = this.mongoConfiguration.getInputCollectionName(); // 输入数据的集合
        getMongo(); // 初始化
        getConditionQuery(); // 初始化
        getFieldQuery(); // 初始化
    }

    @Override
    public Configuration getConf() {
        return this.mongoConfiguration.getConfiguration();
    }

    public Mongo getMongo() {
        try {
            if (null == this.mongo) {
                this.mongo = this.mongoConfiguration.getMongoConnection();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return mongo;
    }

    public DBObject getConditionQuery() {
        if (null == this.conditionQuery) {
            Map<String, String> conditions = this.mongoConfiguration.getInputConditions();
            BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
            for (Map.Entry<String, String> entry : conditions.entrySet()) {
                if (entry.getValue() != null) {
                    builder.append(entry.getKey(), entry.getValue());
                } else {
                    builder.push(entry.getKey());
                }
            }
            if (builder.isEmpty()) {
                this.conditionQuery = new BasicDBObject();
            } else {
                this.conditionQuery = builder.get();
            }
        }
        return this.conditionQuery;
    }

    public DBObject getFieldQuery() {
        if (fieldQuery == null) {
            String[] fields = this.mongoConfiguration.getInputFieldNames();
            if (fields != null && fields.length > 0) {
                BasicDBObjectBuilder builder = new BasicDBObjectBuilder();
                for (String field : fields) {
                    builder.push(field);
                }
                fieldQuery = builder.get();
            } else {
                fieldQuery = new BasicDBObject();
            }
        }
        return fieldQuery;
    }

    protected DBCollection getDBCollection() {
        DB db = getMongo().getDB(this.databaseName);
        if (this.mongoConfiguration.isEnableAuth()) {
            String username = this.mongoConfiguration.getUsername();
            String password = this.mongoConfiguration.getPassword();
//            if (!db.authenticate(username, password.toCharArray())) {
//                throw new RuntimeException("authenticate failure with the username:" + username + ",pwd:" + password);
//            }
        }
        return db.getCollection(collectionName);
    }

    protected void closeConnection() {
        try {
            if (null != this.mongo) {
                this.mongo.close();
                this.mongo = null;
            }
        } catch (Exception e) {
            LOG.debug("Exception on close", e);
        }
    }
}
