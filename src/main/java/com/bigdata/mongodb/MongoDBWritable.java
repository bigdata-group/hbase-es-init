package com.bigdata.mongodb;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

public interface MongoDBWritable {
    /**
     * 往mongodb的集合中写数据
     * 
     * @param collection
     * @throws MongoException
     */
    public void write(DBCollection collection) throws MongoException;

    /**
     * 获取要写的mongoDB对象
     * 
     * @param old
     * @return
     * @throws MongoException
     */
    public DBObject fetchWriteDBObject(DBObject old) throws MongoException;

    /**
     * 从mongodb的集合中读数据
     * 
     * @param collection
     * @throws MongoException
     */
    public void readFields(DBObject object) throws MongoException;
}
