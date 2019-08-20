package com.bigdata.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * @author jiwenlong
 *
 */
public class ESClient {

    private static Client client;
    
    private static byte[] LOCK = new byte[0];
    
    public static Client getEsClient(String clusterName, int port, String... nodeHosts) throws Exception {
    	synchronized (LOCK) {
			if (client == null) {
				Settings settings = Settings.builder().put("client.transport.sniff", true).put("cluster.name", clusterName).build();
				try {
					PreBuiltTransportClient preClient = new PreBuiltTransportClient(settings);
					for (String nodeHost : nodeHosts){
						preClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(nodeHost), port));
					}
					client = preClient;
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
			}
		}
		return client;
    }
    
//    public static void main(String[] args) throws Exception{
//		getEsClient("elasticsearch", 9300, "10.10.4.108");
//		
//		ESClient.client.close();
//	}
}
