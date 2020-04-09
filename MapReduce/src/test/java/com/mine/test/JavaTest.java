package com.mine.test;

import com.mine.utils.ConfProperty;
import com.mine.utils.PropertyGet;
import org.junit.Test;


public class JavaTest {

  @Test
  public void propertyTest() {
    PropertyGet get = new PropertyGet("conf.properties");
    get.set("aa", "jfdls");
    System.out.println(get.get("queue.name") + " ----------- " + get.get("aa"));
  }

  @Test
  public void confPropertyTest() {
    PropertyGet get = new PropertyGet("conf.properties");
    String cluster = get.get("dfs.cluster");
    ConfProperty confProperty = new ConfProperty("conf.properties");
    System.out.println(confProperty.conf().get("mapred.job.queue.name") + "\n" +
      confProperty.conf().get("fs.defaultFS") + "\n" +
      confProperty.conf().get("dfs.nameservices") + "\n" +
      confProperty.conf().get("dfs.ha.namenodes." + cluster) + "\n" +
      confProperty.conf().get("dfs.namenode.rpc-address." + cluster + ".nn1") + "\n" +
      confProperty.conf().get("dfs.namenode.rpc-address." + cluster + ".nn1") + "\n" +
      confProperty.conf().get("dfs.client.failover.proxy.provider." + cluster) + "\n"
    );
    System.out.println(confProperty.conf());
  }
}
