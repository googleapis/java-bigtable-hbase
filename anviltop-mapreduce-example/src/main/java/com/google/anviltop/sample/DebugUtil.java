package com.google.anviltop.sample;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class DebugUtil {
  private static final Log LOG = LogFactory.getLog(DebugUtil.class);

  public static void printSystemProperties() {
    TreeSet<Object> keys = new TreeSet<Object>(System.getProperties().keySet());
    for (Object key : keys) {
      LOG.info(key + " = " + System.getProperty(key.toString()));
    }
  }
  
  public static void printConf(Configuration conf){
    System.out.println("\n\n\n=============");
    Map<String, String> props = new TreeMap<String, String>();
    for(Iterator<Entry<String, String>> iterator = conf.iterator(); iterator.hasNext(); ){
      Entry<String, String> entry = iterator.next();
      props.put(entry.getKey(), entry.getValue());
    }
    for (Entry<String, String> entry : props.entrySet()) {
      System.out.println(entry.getKey() + " = " + entry.getValue());
    }
    System.out.println("=============\n\n\n");

  }

}
