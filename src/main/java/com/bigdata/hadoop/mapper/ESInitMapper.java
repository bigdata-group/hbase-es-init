package com.bigdata.hadoop.mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;

import com.bigdata.elasticsearch.ESClient;

/**
 * ES init mapper
 */
public class ESInitMapper extends TableMapper<NullWritable, NullWritable> {

    private Client client;
    private static final Log LOG = LogFactory.getLog(ESInitMapper.class);
    private BulkRequestBuilder bulkRequestBuilder;
    private String index;
    private String type;
    private List<String> fields;

    /**
     * init ES
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            String clusterName = context.getConfiguration().get("es.cluster.name");
            String hosts = context.getConfiguration().get("es.cluster.hosts");
            int port = Integer.parseInt(context.getConfiguration().get("es.cluster.port"));
            client = ESClient.getEsClient(clusterName,port,hosts.split(","));
            bulkRequestBuilder = client.prepareBulk();
            bulkRequestBuilder.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
            index = context.getConfiguration().get("index");
            type = context.getConfiguration().get("tableName");
            String typeFields = context.getConfiguration().get("fields");
        	fields = Arrays.asList(typeFields.split(","));
        } catch (Exception ex) {
        	System.out.println(ex);
            LOG.error("init ES Client error:" + ex.getMessage());
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result values, Context context) throws IOException, InterruptedException {
        Map<String, Object> json = new HashMap<String, Object>();
        String rowKey = Bytes.toString(values.getRow());
        if (rowKey.indexOf("_AYX") > 0){
        	//json.put("rowkey", rowKey);
            for (KeyValue kv : values.raw()) {
            	String keyHbase = Bytes.toString(kv.getQualifier());
            	if (fields.contains(keyHbase))
            	{
            		String valueHbase = Bytes.toString(kv.getValue());
            		json.put(keyHbase, valueHbase);
            	}
            }
            if (json.size() > 0){
            	addUpdateBuilderToBulk(client.prepareUpdate(index, type, rowKey).setDocAsUpsert(true).setDoc(json));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (0 < bulkRequestBuilder.numberOfActions()) {
            try {
                bulkRequest();
            } catch (Exception ex) {
                LOG.error("Bulk " + index + "index error :" + ex.getMessage());
            }
        }
        //关闭es
        client.close();
    }

    /**
     * execute bulk process
     * @throws Exception
     */
    private void bulkRequest() throws Exception {
        BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
        if (!bulkResponse.hasFailures()) {
            bulkRequestBuilder = client.prepareBulk();
        }
    }

    /**
     *add prepare update date  to builder
     * @param builder
     */
    private synchronized void addUpdateBuilderToBulk(UpdateRequestBuilder builder) {
        try {
            if (bulkRequestBuilder.numberOfActions() != 0 && (bulkRequestBuilder.numberOfActions() % 1000 == 0)) {
                bulkRequest();
            }
            bulkRequestBuilder.add(builder);
        } catch (Exception ex) {
            LOG.error("Bulk" + index + "index error :" + ex.getMessage());
        }
    }
}
