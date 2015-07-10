package com.xirong.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CustomerLoggerFactory {
	public static final int LOG_PER = 3000;
	public static final int LOG_PER_C = 10000;
	public static final int ERROR_LOG_PER = 100;
	public static final int PHOENIX_TIME_PER =100;
	public static final int KAFKA_ERROR_PER =10000;
	public static Logger LOGGER(){
		return LoggerFactory.getLogger("default_logger");
	}
	public static Logger LOGGER(String name){
		return LoggerFactory.getLogger(name);
	}
	public static Logger LOGGER(Class<?> class1){
		return LoggerFactory.getLogger(class1);
	}
}
