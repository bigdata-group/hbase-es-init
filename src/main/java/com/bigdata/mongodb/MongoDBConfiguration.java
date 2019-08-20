package com.bigdata.mongodb;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import com.mongodb.Mongo;
import com.mongodb.ServerAddress;

public class MongoDBConfiguration {
    public static final String BIND_HOST_PROPERTY = "mapreduce.mongo.host";
    public static final String BIND_PORT_PROPERTY = "mapreduce.mongo.port";
    public static final String AUTH_ENABLE_PROPERTY = "mapreduce.mongo.auth.enable";
    public static final String USERNAME_PROPERTY = "mapreduce.mongo.username";
    public static final String PASSWORD_PROPERTY = "mapreduce.mongo.password";
    public static final String PARTITION_PROPERTY = "mapreduce.mongo.partition";

    public static final String INPUT_DATABASE_NAME_PROPERTY = "mapreduce.mongo.input.database.name";
    public static final String INPUT_COLLECTION_NAME_PROPERTY = "mapreduce.mongo.input.collection.name";
    public static final String INPUT_FIELD_NAMES_PROPERTY = "mapreduce.mongo.input.field.names";
    public static final String INPUT_CONDITIONS_PROPERTY = "mapreduce.mongo.input.conditions";
    public static final String INPUT_CLASS_PROPERTY = "mapreduce.mongo.input.class";

    public static final String OUTPUT_DATABASE_NAME_PROPERTY = "mapreduce.mongo.output.database.name";
    public static final String OUTPUT_COLLECTION_NAME_PROPERTY = "mapreduce.mongo.output.collection.name";
    // 在recordwriter中到底是否调用fetch方法，默认调用。如果设置为不调用，那么就直接使用writer方法
    public static final String OUTPUT_USE_FETCH_METHOD_PROPERTY = "mapreduce.mongo.output.use.fetch.method";

    private Configuration conf;

    public MongoDBConfiguration(Configuration conf) {
        this.conf = conf;
    }

    /**
     * 获取Configuration对象
     * 
     * @return
     */
    public Configuration getConfiguration() {
        return this.conf;
    }

    /**
     * 设置连接信息
     * 
     * @param host
     * @param port
     * @return
     */
    public MongoDBConfiguration configureDB(String host, int port) {
        return this.configureDB(host, port, false, null, null);
    }

    /**
     * 设置连接信息
     * 
     * @param host
     * @param port
     * @param enableAuth
     * @param username
     * @param password
     * @return
     */
    public MongoDBConfiguration configureDB(String host, int port, boolean enableAuth, String username, String password) {
        this.conf.set(BIND_HOST_PROPERTY, host);
        this.conf.setInt(BIND_PORT_PROPERTY, port);
        if (enableAuth) {
            this.conf.setBoolean(AUTH_ENABLE_PROPERTY, true);
            this.conf.set(USERNAME_PROPERTY, username);
            this.conf.set(PASSWORD_PROPERTY, password);
        }
        return this;
    }

    /**
     * 获取MongoDB的连接对象Connection对象
     * 
     * @return
     * @throws UnknownHostException
     */
    public Mongo getMongoConnection() throws UnknownHostException {
        return new Mongo(new ServerAddress(this.getBindHost(), this.getBindPort()));
    }

    /**
     * 获取设置的host
     * 
     * @return
     */
    public String getBindHost() {
        return this.conf.get(BIND_HOST_PROPERTY, "localhost");
    }

    /**
     * 获取设置的port
     * 
     * @return
     */
    public int getBindPort() {
        return this.conf.getInt(BIND_PORT_PROPERTY, 27017);
    }

    /**
     * 获取是否开启安全验证，默认的Mongodb是不开启的。
     * 
     * @return
     */
    public boolean isEnableAuth() {
        return this.conf.getBoolean(AUTH_ENABLE_PROPERTY, false);
    }

    /**
     * 获取完全验证所需要的用户名
     * 
     * @return
     */
    public String getUsername() {
        return this.conf.get(USERNAME_PROPERTY);
    }

    /**
     * 获取安全验证所需要的密码
     * 
     * @return
     */
    public String getPassword() {
        return this.conf.get(PASSWORD_PROPERTY);
    }

    public String getPartition() {
        return conf.get(PARTITION_PROPERTY, "|");
    }

    public MongoDBConfiguration setPartition(String partition) {
        conf.set(PARTITION_PROPERTY, partition);
        return this;
    }

    public String getInputDatabaseName() {
        return conf.get(INPUT_DATABASE_NAME_PROPERTY, "test");
    }

    public MongoDBConfiguration setInputDatabaseName(String databaseName) {
        conf.set(INPUT_DATABASE_NAME_PROPERTY, databaseName);
        return this;
    }

    public String getInputCollectionName() {
        return conf.get(MongoDBConfiguration.INPUT_COLLECTION_NAME_PROPERTY, "test");
    }

    public void setInputCollectionName(String tableName) {
        conf.set(MongoDBConfiguration.INPUT_COLLECTION_NAME_PROPERTY, tableName);
    }

    public String[] getInputFieldNames() {
        return conf.getStrings(MongoDBConfiguration.INPUT_FIELD_NAMES_PROPERTY);
    }

    public void setInputFieldNames(String... fieldNames) {
        conf.setStrings(MongoDBConfiguration.INPUT_FIELD_NAMES_PROPERTY, fieldNames);
    }

    public Map<String, String> getInputConditions() {
        Map<String, String> result = new HashMap<String, String>();
        String[] conditions = conf.getStrings(INPUT_CONDITIONS_PROPERTY);
        if (conditions != null && conditions.length > 0) {
            String partition = this.getPartition();
            String[] values = null;
            for (String condition : conditions) {
                values = condition.split(partition);
                if (values != null && values.length == 2) {
                    result.put(values[0], values[1]);
                } else {
                    result.put(condition, null);
                }
            }
        }
        return result;
    }

    public void setInputConditions(Map<String, String> conditions) {
        if (conditions != null && conditions.size() > 0) {
            String[] values = new String[conditions.size()];
            String partition = this.getPartition();
            int k = 0;
            for (Map.Entry<String, String> entry : conditions.entrySet()) {
                if (entry.getValue() != null) {
                    values[k++] = entry.getKey() + partition + entry.getValue();
                } else {
                    values[k++] = entry.getKey();
                }
            }
            conf.setStrings(INPUT_CONDITIONS_PROPERTY, values);
        }
    }

    public Class<? extends MongoDBWritable> getValueClass() {
        return conf.getClass(INPUT_CLASS_PROPERTY, MongoDBInputFormat.NullMongoDBWritable.class, MongoDBWritable.class);
    }

    public void setInputClass(Class<? extends DBWritable> inputClass) {
        conf.setClass(MongoDBConfiguration.INPUT_CLASS_PROPERTY, inputClass, DBWritable.class);
    }

    public String getOutputDatabaseName() {
        return conf.get(OUTPUT_DATABASE_NAME_PROPERTY, "test");
    }

    public MongoDBConfiguration setOutputDatabaseName(String databaseName) {
        conf.set(OUTPUT_DATABASE_NAME_PROPERTY, databaseName);
        return this;
    }

    public String getOutputCollectionName() {
        return conf.get(MongoDBConfiguration.OUTPUT_COLLECTION_NAME_PROPERTY, "test");
    }

    public void setOutputCollectionName(String tableName) {
        conf.set(MongoDBConfiguration.OUTPUT_COLLECTION_NAME_PROPERTY, tableName);
    }

    public boolean isEnableUseFetchMethod() {
        return conf.getBoolean(OUTPUT_USE_FETCH_METHOD_PROPERTY, true);
    }

    public void setOutputUseFetchMethod(boolean useFetchMethod) {
        conf.setBoolean(OUTPUT_USE_FETCH_METHOD_PROPERTY, useFetchMethod);
    }
}
