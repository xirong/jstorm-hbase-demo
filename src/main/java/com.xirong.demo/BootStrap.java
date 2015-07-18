package com.xirong.demo;

import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BootStrap {

    @SuppressWarnings("unused")
    private static final Logger LOGGER = CustomerLoggerFactory.LOGGER();

    public static void main(String[] args) {
        try {
            //在nimbus机器上，从命令行寻找响应topology配置,spout,bolt,worker配置 暂不支持同时启动多个topology
            String configPath = args[0];
            Properties config = ConfigUtils.init(configPath, false);
            ((ILogTopology) BootStrap.SpringContext.instance.getBean(config.getProperty("topology.bean.name"))).start(config);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class Log4JInitBean {
        public static void init() {
            File file = new File("/home/work/jstorm_cluster/jstorm-0.9.6.3/biglog_conf/log4j.xml");
            if (file.exists()) {
                System.out.println("log4j init from /home/work/jstorm_cluster/jstorm-0.9.6.3/biglog_conf/log4j.xml");
                DOMConfigurator.configure("/home/work/jstorm_cluster/jstorm-0.9.6.3/biglog_conf/log4j.xml");
            } else {
                System.out.println("log4j init from default");
            }
        }
    }

    static class SpringContext {
        static {
            Log4JInitBean.init();
        }

        public static ApplicationContext instance = new ClassPathXmlApplicationContext(
                "/config/demo-beans-context.xml");
    }

    private static CountDownLatch downLatch = new CountDownLatch(1);

    public static void wait4amoument(long mill) {
        try {
            downLatch.await(mill, TimeUnit.MICROSECONDS);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    public static <T> T getBean(Class<T> clazz) {
        return SpringContext.instance.getBean(clazz);
    }

    public static Object getBean(String name) {
        return SpringContext.instance.getBean(name);
    }

}
