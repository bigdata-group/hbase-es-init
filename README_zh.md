# 使用Mapreduce将hbase表中的数据全量导入ElasticSearch

对于做Hbase+ElasticSearch的项目来说，数据同步以及初始化Hbase中的数据到Elasticsearch都是经常要做的事情，我在之前的博文中已经介绍过如何自己编写一个Hbase组件来做ElasticSearch的同步，那么今天我想介绍一下，如何全量的将数据一次性从Hbase中同步到ElasticSearch中去。当然，我们需要用到一个MapReduce即可。
完整代码托管在码云上面，[点击查看完整代码](https://git.oschina.net/eminem89/Hbase-ElasticSearch-Init.git)，欢迎到我的CSDN博客留言[点击连接到我的csdn博客](http://blog.csdn.net/fxsdbt520)

- **编写导入数据到ElasticSearch的Mapreduce**
- **运行Mapreduce**
- **总结**

>- **编写导入数据到ElasticSearch的Mapreduce**
>我们写mapreduce之前得考虑一下导入的逻辑，Hbase中有一张目标表A，然后ES中有一个目标索引B。我们需要将A表中的数据全量的导入到B索引中去。那么，在map阶段之前，我们需要先scan A表中的全量数据，然后利用map阶段读取到这些数据，并将这些数据通过ES的java api bulk到B索引中去即可，因为我们不需要做分析统计这些数据，只要拿到数据进行bulk即可，所以不需要reducer。一个map就可以搞定。关键是，mapreduce支持直接从hbase读取数据，我们在定义mapper的时候需要继承一个TableMapper类，这样我们就可以轻松搞定了，好了，话不多说，请看关键代码。

```
package org.eminem.hadoop.mapper;

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
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.eminem.elasticsearch.ESClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * ES init mapper
 */
public class ESInitMapper extends TableMapper<NullWritable, NullWritable> {

    private Client client;
    private static final Log LOG = LogFactory.getLog(ESInitMapper.class);
    private BulkRequestBuilder bulkRequestBuilder;
    private String index;
    private String columnFamily;

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
            String host = context.getConfiguration().get("es.cluster.host");
            int port = Integer.parseInt(context.getConfiguration().get("es.cluster.port"));
            client = ESClient.getEsClient(clusterName,host,port);
            bulkRequestBuilder = client.prepareBulk();
            bulkRequestBuilder.setRefresh(true);
            index = context.getConfiguration().get("index");
            columnFamily = context.getConfiguration().get("columnFamily");
        } catch (Exception ex) {
            LOG.error("init ES Client error:" + ex.getMessage());
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result values, Context context) throws IOException, InterruptedException {
        Map<String, Object> json = new HashMap<String, Object>();
        Map<String, Object> infoJson = new HashMap<String, Object>();
        String rowKey = Bytes.toString(values.getRow());
        for (KeyValue kv : values.raw()) {
            String keyHbase = Bytes.toString(kv.getQualifier());
            String valueHbase = Bytes.toString(kv.getValue());
            json.put(keyHbase, valueHbase);
        }
        // set Family(you can do not set or change zhe Family name)
        infoJson.put(columnFamily, json);
        addUpdateBuilderToBulk(client.prepareUpdate(index, index, rowKey).setDocAsUpsert(true).setDoc(infoJson));
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
            if (bulkRequestBuilder.numberOfActions() != 0 && (bulkRequestBuilder.numberOfActions() % 500 == 0)) {
                bulkRequest();
            }
            bulkRequestBuilder.add(builder);
        } catch (Exception ex) {
            LOG.error("Bulk" + index + "index error :" + ex.getMessage());
        }
    }
}

 
```

上面就是mapper的主要逻辑，ESInitMapper extends TableMapper，由于没有reducer所以，mapper的keyout和valueout都是nullwritable，然后再map方法中，拿到的就是hbase表中一行记录，对这行记录进行组装之后，放到一个缓冲池中，如果缓冲池中超过500的阀值，那么就会进行一次bulk。这样做的目的就是，不会过于频繁的写入ES导致性能有问题，而是批量的进行bulk。下面还有一段关键的代码，就是定义job以及如何启动job，因为平时我们写mapreduce的时候，都是读取hdfs上的文件。但是，今天我们读取的是Hbase中的表，到底要怎么写呢？请看下面这段代码。

```
 package org.eminem.hadoop;
 
 import org.apache.commons.lang.StringUtils;
 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.hbase.client.Scan;
 import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
 import org.apache.hadoop.mapreduce.Job;
 import org.eminem.hadoop.mapper.ESInitMapper;
 import org.eminem.hbase.HbaseClient;
 import org.eminem.utils.PropertiesUtil;
 
 import java.io.IOException;
 
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
         System.out.println("es.cluster.host:" + PropertiesUtil.getStringByKey("es.cluster.port"));
         System.out.println("es.cluster.host:" + PropertiesUtil.getStringByKey("es.cluster.host"));
         System.out.println("hbase.zookeeper.quorum:" + PropertiesUtil.getStringByKey("hbase.zookeeper.quorum"));
         System.out.println("hbase.zookeeper.property.clientPort:"+ PropertiesUtil.getStringByKey("hbase.zookeeper.property.clientPort"));
         Configuration conf = HbaseClient.getConfiguration(PropertiesUtil.getStringByKey("hbase.zookeeper.quorum"), PropertiesUtil.getStringByKey("hbase.zookeeper.property.clientPort"));
         conf.set("index", index);
         conf.set("columnFamily", columnFamily);
         conf.set("es.cluster.name", PropertiesUtil.getStringByKey("es.cluster.name"));
         conf.set("es.cluster.port", PropertiesUtil.getStringByKey("es.cluster.port"));
         conf.set("es.cluster.host", PropertiesUtil.getStringByKey("es.cluster.host"));
         Job job = Job.getInstance(conf, "ESInit " + index + " Call Job");
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

 
```

> scan.setCaching(500); //  这里是设置hbase的缓存大小。
  scan没有加任何条件，所以说scan的全量的数据，但是如果全量数据量很大，怎么办？所以我们要设置缓冲500，setCaching设置的值为每次rpc的请求记录数（每次从服务器端读取的行数，默认为配置文件中设置的值），默认是1，cache大可以优化性能，但是太大了会花费很长的时间进行一次传输。
  
 > scan.setCacheBlocks(false); // don't set to true for MR jobs
 为是否缓存块，默认缓存，我们分内存，缓存和磁盘，三种方式，一般数据的读取为内存->缓存->磁盘，当MR的时候为非热点数据，因此不需要缓存。

> // 设置map过程
        TableMapReduceUtil.initTableMapperJob(
                tableName,      // input table
                scan,              // Scan instance to control CF and attribute selection
                ESInitMapper.class,   // mapper class
                null,              // mapper output key
                null,              // mapper output value
                job);
                直接调用TableMapReduceUtil来初始化Mapper，第一个参数表示要读取hbase中的表名称，第二个参数，scan就是扫描这张表，可以根据事先定义好的scan对数据进行过滤操作，接下来四个参数就很好理解了，分别是mapper的class，mapper的输出key值，以及mapper的输出value，最后就是job。由于这个场景前面说到过，我们不需要reducer阶段，所以mapper的输出key值=null，以及mapper的输出value=null即可。

> TableMapReduceUtil.initTableReducerJob(
                tableName,      // output table
                null,             // reducer class
                job);
        job.setNumReduceTasks(0);



- **运行Mapreduce**
>maven 打包

```
mvn clean package
```

>执行shell命令

```
 hadoop jar hbase-es-init-1.3.jar  daisy_risk risk_ent_encounter info 
 
```

- **总结**
>这是一个很简单的mapreduce，主要就是讲述如何从hbase中读取数据，然后进行mapreduce作业，我们可以根据实际的场景，使用TableMapReduceUtil这个工具类来初始化mapper和reducer，并根据scan来过滤提取想要的数据。以下就是读取hbase中的数据来进行mapreduce的步骤。

1、编写Mapper继承TableMapper。
2、如果有Reducer那么需要编写Reducer继承TableReducer。
3、使用TableMapReduceUtil工具类来初始化mapper和reducer。
4、在初始化mapper之前，使用scan来对hbase中的表进行过滤和提取。



        
 




