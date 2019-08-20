package com.bigdata.mongodb;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoDBRecordReader<T extends MongoDBWritable> extends RecordReader<LongWritable, T> {
    private Class<? extends MongoDBWritable> valueClass;
    private LongWritable key;
    private T value;
    private long pos;
    private Configuration conf;
    private MongoDBInputFormat.MongoDBInputSplit split;
    private DBCollection collection;
    private DBObject conditionQuery;
    private DBObject fieldQuery;
    private DBCursor cursor;

    public MongoDBRecordReader(MongoDBInputFormat.MongoDBInputSplit split, Class<? extends MongoDBWritable> valueClass, Configuration conf, DBCollection collection, DBObject conditionQuery,
            DBObject fieldQuery) {
        this.split = split;
        this.valueClass = valueClass;
        this.collection = collection;
        this.conditionQuery = conditionQuery;
        this.fieldQuery = fieldQuery;
        this.conf = conf;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // do nothing
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        try {
            if (key == null) {
                key = new LongWritable();
            }
            if (value == null) {
                value = (T) ReflectionUtils.newInstance(valueClass, conf);
            }
            if (null == cursor) {
                cursor = executeQuery();
            }
            if (!cursor.hasNext()) {
                return false;
            }

            key.set(pos + split.getStart()); // 设置key
            value.readFields(cursor.next()); // 设置value
            pos++;
        } catch (Exception e) {
            throw new IOException("Exception in nextKeyValue", e);
        }
        return true;
    }

    protected DBCursor executeQuery() {
        try {
            return collection.find(conditionQuery, fieldQuery).skip((int) split.getStart()).limit((int) split.getLength());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public T getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return pos;
    }

    @Override
    public void close() throws IOException {
        if (collection != null) {
            collection.getDB().getMongo().close();
        }
    }

}
