package com.xirong.demo;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

import java.io.IOException;
import java.util.Properties;

public interface ILogTopology {
	public void start(Properties properties) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, IOException ;
}
