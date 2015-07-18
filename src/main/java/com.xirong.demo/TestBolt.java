package com.xirong.demo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public  class TestBolt extends BaseRichBolt {

    private static final Logger LOGGER = CustomerLoggerFactory.LOGGER(TestBolt.class);
    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String xx = input.getString(0);
        LOGGER.info(String.format("receive from spout ,num is : %d", xx));

        // 发送ack信息告知spout 完成处理的消息 ，如果下面的hbase的注释代码打开了，则必须等到插入hbase完毕后才能发送ack信息，这段代码需要删除
        this.collector.ack(input);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 下面是往hbase中写数据的地方，如果不需要这块，完全不用打开注释
//        Statement stmt = null;
//        PreparedStatement statement = null;
//        ResultSet rset = null;
//        Connection con = null;
//        try {
//            // 10.35.66.72:2181 hbase zookeeper的地址，采用Phoenix连接
//            LOGGER.info("before  Connection con = DriverManager.getConnection(\"jdbc:phoenix:10.35.66.72:2181\");");
//            con = DriverManager.getConnection("jdbc:phoenix:10.35.66.72:2181");
//            stmt = con.createStatement();
//            LOGGER.info("after Connection con = DriverManager.getConnection(\"jdbc:phoenix:10.35.66.72:2181\");");
//
//            StringBuilder sb = new StringBuilder();
//            sb.append("upsert into ol.test values (").append(pendNum.incrementAndGet()).append(",'Hello").append(pendNum.incrementAndGet()).append("')");
//            stmt.executeUpdate(sb.toString());
//
//            LOGGER.info("before con.commit();");
//            con.commit();
//            LOGGER.info("after con.commit();");
//
//            statement = con.prepareStatement("select * from ol.test");
//            LOGGER.info("before statement.executeQuery();");
//            rset = statement.executeQuery();
//            LOGGER.info("after statement.executeQuery();");
//            while (rset.next()) {
//                System.out.println(rset.getString("mycolumn"));
//                LOGGER.info("sqlstr from hbase:" + rset.getString("mycolumn"));
//            }
//
//            if (pendNum.incrementAndGet() >= 1) {
//                sb.delete(0, sb.length());
//                sb.append("delete from ol.test where mykey=").append(pendNum.incrementAndGet() - 1);
//                int deleNum = stmt.executeUpdate(sb.toString());
//                LOGGER.info("after delete from ol.test where mykey=" + (pendNum.incrementAndGet() - 1) + " affect：" + deleNum);
//            }
//            statement.close();
//            con.close();
//
//            // 发送ack信息告知spout 完成处理的消息
//            this.collector.ack(input);
//
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//            LOGGER.info("execute InterruptedException:", e);
//        } catch (SQLException e) {
//            e.printStackTrace();
//            LOGGER.info("execute SQLException:", e);
//        } finally {
//            try {
//                statement.close();
//                con.close();
//            } catch (Exception ex) {
//
//            }
//        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }


}