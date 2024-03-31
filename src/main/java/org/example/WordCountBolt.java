package org.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
public class WordCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private HashMap<String, Integer> counts = new HashMap<>();

    @Override
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        int count = counts.getOrDefault(word, 0);
        count++;
        counts.put(word, count);
        this.collector.emit(new Values(word, count));
        System.out.println("Word: " + word + ", Count: " + count); // Print the word count to the console
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
