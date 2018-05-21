package kafka.producer;

import kafka.producer.Bolts.DataCountBolt;

import java.util.Arrays;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TridentKafkaState;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import org.elasticsearch.storm.EsBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import storm.kafka.bolt.KafkaBolt;


public class ProcessTopology {

    private static final String KAFKA_SPOUT_ID="kafka-spout";

    private static final String zks = "node01.das3.liacs.nl:2181,node02.das3.liacs.nl:2181";
    private static final String CONSUME_TOPIC="topic1";
    private static final String ZK_ROOT="/usr/hdp/current/zookeeper";
    private static final String ZK_ID="unknown";
    //此处zkid具体对应什么未知
    private static final String TOPOLOGY_NAME="testtopology";





    public static void main(String[] args) throws InterruptedException{



        BrokerHosts brokerHosts=new ZkHosts(zks);
        SpoutConfig spoutConfig=new SpoutConfig(brokerHosts,CONSUME_TOPIC,ZK_ROOT,ZK_ID);

        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.zkServers = Arrays.asList(new String[] {"node1", "node2"});
        spoutConfig.zkPort = 2181;


        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout(KAFKA_SPOUT_ID,new KafkaSpout(spoutConfig),3);//需要注意的是，spout的并行度不能超过topic的partition个数！
        builder.setBolt("intermediateBolt",new DataCountBolt(),1).allGrouping(KAFKA_SPOUT_ID);

//        方法1:直接写入es
//        似乎esbolt失效，setbolt无法解析esbolt
//        Map esconf = new HashMap();
//        esconf.put("es.mapping.id", "sentenceId");
//        builder.setBolt("esbolt",new EsBolt("target_name",esconf)).allGrouping("try_bolt");

//        方法二：回写到kafka，通过logstash写入es
        KafkaBolt bolt = new KafkaBolt();
        bolt.withTopicSelector(new DefaultTopicSelector("aftertopic")).withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
        builder.setBolt("kfkbolt", bolt, 1).shuffleGrouping("intermediateBolt");

        //设置kafka producer的配置
        Config conf = new Config();
        Properties props = new Properties();
        props.put("metadata.broker.list", "fs.das3.liacs.nl:9092,node01:9092");
        props.put("producer.type","async");
        props.put("request.required.acks", "0"); // 0 ,-1 ,1
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);
        conf.put("topic","aftertopic");
//        如果生成数据丢到kkf，添加对应的topic
//        config.put("topic", PRODUCT_TOPIC);

        conf.setNumWorkers(1);

        LocalCluster cluster=new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME,conf,builder.createTopology());

        //100秒后关闭这个topo
        Thread.sleep(100000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
