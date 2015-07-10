package com.xirong.demo;

import backtype.storm.Config;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class ConfigUtils {
	private static Logger logger = CustomerLoggerFactory.LOGGER();
	public static Properties init(String path,boolean isClassPath){
		Properties  properties = new Properties();
		InputStream in=null;
		try {
			 
			 if(!isClassPath){
				 in = new FileInputStream(path);
			 } else {
				 in = ConfigUtils.class.getClassLoader().getResourceAsStream(path);
			}			
			 properties.load(in);
		} catch (Exception e) {
			logger.error("load properties error ",e);
		}finally{
			if(in!=null){
				try {
					in.close();
				} catch (IOException e) {
					logger.error("close file error ",e);
				}
			}
			logger.info("load proerties"+path+","+isClassPath+","+properties);
		}
		return properties;
	}
	
	public static Config  getStormConfig(Properties config){
		Config conf = new Config();
		if ("true".equals(config.getProperty("storm.localmode", "true"))) {
			conf.setDebug(Boolean.valueOf(config.getProperty("storm.debug", "false")));
			conf.setMaxTaskParallelism(3);
		} else {
			Properties stormProperties = ConfigUtils.init(config.getProperty("storm.config.path"), false);
			for( Map.Entry<Object,Object> entry : stormProperties.entrySet()){
				String key = entry.getKey().toString();
				String value = entry.getValue().toString();
				if(Character.isDigit(value.charAt(0))){
					conf.put(key, Long.valueOf(value));
				}else if("true".equals(value) || "false".equals(value)){
					conf.put(key, Boolean.valueOf(value));
				}else {
					conf.put(key, value);
				}			
			}
			
		}
		if(!conf.containsKey("topology.workers")){
			conf.setNumWorkers(Integer.valueOf(config.getProperty("storm.worker.num")));
		}
		return conf;
	}
}

