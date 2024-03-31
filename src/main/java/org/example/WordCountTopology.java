package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("sentence-spout", new SentenceSpout());
        builder.setBolt("split-bolt", new SplitSentenceBolt()).shuffleGrouping("sentence-spout");
        builder.setBolt("count-bolt", new WordCountBolt()).fieldsGrouping("split-bolt", new Fields("word"));

        Config config = new Config();
        config.setDebug(true);

        // Run topology in local mode
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count-topology", config, builder.createTopology());
        Thread.sleep(10000); // Run for 10 seconds
        cluster.shutdown();
    }
}
