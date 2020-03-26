package com.travelport.storm.adapter;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AdapterTopology implements ApplicationRunner {

    private static final String BOLT = "kinesisProducerBolt";
    private static final String SPOUT = "cars-kafka-spout";
    private static final String TOPOLOGY = "kafka-kinesis-storm-adapter";

    @Value("${kafka.message.topic}")
    private String topic;

    @Value("${kafka.message.consumer.group}")
    private String consumerGroup;

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Autowired
    private KinesisProducerBolt kinesisProducerBolt;

    public static void main(String[] args) {
        SpringApplication.run(AdapterTopology.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        KafkaSpoutConfig<String, String> spoutConfig = KafkaSpoutConfig
                .builder(bootstrapServers, topic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup)
                .build();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT, new KafkaSpout<String, String>(spoutConfig), 1);
        // global grouping vs shuffle grouping ?
        builder.setBolt(BOLT, kinesisProducerBolt).shuffleGrouping(SPOUT);

        Config config = new Config();
        config.setDebug(false);
        config.put(Config.TOPOLOGY_DEBUG, false);

        // Local Cluster, FYI:   
        // Running the Topology in Local mode, Storm does not store Offset in ZK.
        new LocalCluster().submitTopology(TOPOLOGY, config, builder.createTopology());
    }
}
