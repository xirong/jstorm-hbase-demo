package com.xirong.demo;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestSpout extends BaseRichSpout {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestSpout.class);
    static AtomicInteger sAtomicInteger = new AtomicInteger(0);
    static AtomicInteger pendNum = new AtomicInteger(0);
    private int sqnum;
    SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        sqnum = sAtomicInteger.incrementAndGet();
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
       while (true) {
            int a = pendNum.incrementAndGet();
            LOGGER.info(String.format("spount %d,pendNum %d", sqnum, a));
            this.collector.emit(new Values("xxxxx:"+a));

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("log"));

    }

    /**
     * 启用 ack 机制，详情参考：https://github.com/alibaba/jstorm/wiki/Ack-%E6%9C%BA%E5%88%B6
     * @param msgId
     */
    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }

    /**
     * 消息处理失败后需要自己处理
     * @param msgId
     */
    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        LOGGER.info("ack fail,msgId"+msgId);
    }

}