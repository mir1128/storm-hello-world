package com.loobo;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReportBolt extends BaseRichBolt {

    private Map<String, Long> counts;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        counts = new HashMap<String, Long>();
    }

    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        Long count = input.getLongByField("count");

        counts.put(word, count);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        System.out.println("----------Final counts------------");
        List<String> keys = new ArrayList<String>();

        keys.addAll(counts.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            System.out.println(key + " : " + counts.get(key));
        }
        System.out.println("----------------------------------");
    }
}

















