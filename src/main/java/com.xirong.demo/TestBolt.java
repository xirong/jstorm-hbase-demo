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

class TestBolt extends BaseRichBolt {

    private static final Logger LOGGER = CustomerLoggerFactory.LOGGER(TestBolt.class);
    static AtomicInteger sAtomicInteger = new AtomicInteger(0);
    static AtomicInteger pendNum = new AtomicInteger(0);
    private int sqnum;
    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        sqnum = sAtomicInteger.incrementAndGet();
        this.collector = collector;

    }

    @Override
    public void execute(Tuple input) {
        String xx = input.getString(0);
        LOGGER.info(String.format("bolt %d,pendNum %d", sqnum, pendNum.incrementAndGet()));
        Statement stmt = null;
        PreparedStatement statement = null;
        ResultSet rset = null;
        Connection con = null;
        try {
            LOGGER.info("before  Connection con = DriverManager.getConnection(\"jdbc:phoenix:10.35.66.72:2181\");");
            con = DriverManager.getConnection("jdbc:phoenix:10.35.66.72:2181");
            stmt = con.createStatement();
            LOGGER.info("after Connection con = DriverManager.getConnection(\"jdbc:phoenix:10.35.66.72:2181\");");

//			stmt.executeUpdate("create table ol.test (mykey integer not null primary key, mycolumn varchar)");
            StringBuilder sb = new StringBuilder();
            sb.append("upsert into ol.test values (").append(pendNum.incrementAndGet()).append(",'Hello").append(pendNum.incrementAndGet()).append("')");
            stmt.executeUpdate(sb.toString());
//			stmt.executeUpdate("upsert into ol.test values (2,'World!')");

            LOGGER.info("before con.commit();");
            con.commit();
            LOGGER.info("after con.commit();");

            statement = con.prepareStatement("select * from ol.test");
            LOGGER.info("before statement.executeQuery();");
            rset = statement.executeQuery();
            LOGGER.info("after statement.executeQuery();");
            while (rset.next()) {
                System.out.println(rset.getString("mycolumn"));
                LOGGER.info("sqlstr from hbase:" + rset.getString("mycolumn"));
            }

            if (pendNum.incrementAndGet() >= 1) {
                sb.delete(0, sb.length());
                sb.append("delete from ol.test where mykey=").append(pendNum.incrementAndGet() - 1);
                int deleNum = stmt.executeUpdate(sb.toString());
                LOGGER.info("after delete from ol.test where mykey=" + (pendNum.incrementAndGet() - 1) + " affect：" + deleNum);
            }
            statement.close();
            con.close();

//            LOGGER.info("before stmt.executeUpdate(\"drop table ol.test\");");
//			stmt.executeUpdate("drop table ol.test");
//            LOGGER.info("after stmt.executeUpdate(\"drop table ol.test\");");

            this.collector.ack(input);

            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOGGER.info("execute InterruptedException:", e);
        } catch (SQLException e) {
            e.printStackTrace();
            LOGGER.info("execute SQLException:", e);
        } finally {
            try {
                statement.close();
                con.close();
            } catch (Exception ex) {

            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }


}