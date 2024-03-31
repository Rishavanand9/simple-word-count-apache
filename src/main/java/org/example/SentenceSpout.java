package org.example;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SentenceSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private String[] sentences = {
            "Apache Storm is a free and open source",
            "distributed realtime computation system",
            "Storm makes it easy to reliably process unbounded streams of data",
            "The Storm project is hosted by the Apache Software Foundation"
    };
    private int index = 0;

    @Override
    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if (index >= sentences.length) {
            index = 0;
        }
        this.collector.emit(new Values(sentences[index]));
        index++;
        try {
            Thread.sleep(1000); // Emit a sentence every second
        } catch (InterruptedException e) {
            // Handle interruption
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}
