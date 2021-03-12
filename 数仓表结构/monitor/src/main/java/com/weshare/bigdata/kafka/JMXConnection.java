package com.weshare.bigdata.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by mouzwang on 2020-12-03 10:23
 */
public class JMXConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(JMXConnection.class);
    private MBeanServerConnection conn;
    private String jmxURL;
    private String ipAndPort = null;

    public JMXConnection() {
    }

    public JMXConnection(String ipAndPort) {
        this.ipAndPort = ipAndPort;
    }

    /**
     * initialize jmx connection
     *
     * @return
     */
    public boolean init() {
        jmxURL = "service:jmx:rmi:///jndi/rmi://" + ipAndPort + "/jmxrmi";
        LOGGER.info("ipAndPort : {}",ipAndPort);
        LOGGER.info("init jmx, jmxUrl: {} , and begin to connect it", jmxURL);
        try {
            JMXServiceURL jmxServiceURL = new JMXServiceURL(jmxURL);
            JMXConnector connector = JMXConnectorFactory.connect(jmxServiceURL,null);
            conn = connector.getMBeanServerConnection();
            if (conn == null) {
                LOGGER.error("get connection return null");
                return false;
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public Object getAttribute(String objName, String objAttr) {
        ObjectName objectName;
        try {
            objectName = new ObjectName(objName);
        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
            return null;
        }
        return getAttribute(objectName,objAttr);
    }

    public Object getAttribute(ObjectName objName, String objAttr) {
        if (conn == null) {
            LOGGER.error("jmx connection is null!");
            return null;
        }
        try {
            return conn.getAttribute(objName, objAttr);
        } catch (MBeanException e) {
            e.printStackTrace();
            return null;
        } catch (AttributeNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (InstanceNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (ReflectionException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * @param topic
     * @return 获取发送量(单个broker的 ， 要计算某个topic的总的发送量就要计算集群中每一个broker之和)
     */
    public long getMsgInCountPerSec(String topic) {
        String objectName = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=" + topic;
        Object val = getAttribute(objectName, "Count");
        String debugInfo = "jmxUrl:" + jmxURL + ",objectName=" + objectName;
        if (val != null) {
            LOGGER.info("{},Count:{}", debugInfo, val);
            return (long) val;
        }
        return 0;
    }

    /**
     * @param topic
     * @return 获取发送的tps，和发送量一样如果要计算某个topic的发送量就需要计算集群中每一个broker中此topic的tps之和
     */
    public double getMsgInTpsPerSec(String topic) {
        String objectName = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=" + topic;
        Object val = getAttribute(objectName, "OneMinuteRate");
        if (val != null) {
            double dVal = ((Double) val).doubleValue();
            return dVal;
        }
        return 0;
    }

    private Set<ObjectName> getEndOffsetObjects(String topic) {
        String objectName;
        objectName = "kafka.log:type=Log,name=LogEndOffset,topic=" + topic + ",partition=*";
        ObjectName objName = null;
        Set<ObjectName> objectNames = null;
        try {
            objName = new ObjectName(objectName);
            objectNames = conn.queryNames(objName, null);
        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return objectNames;
    }

    private int getParId(ObjectName objName) {
        String s = objName.getKeyProperty("partition");
        return Integer.parseInt(s);
    }

    /**
     * @param topic
     * @return 获取topic中每个partition所对应的offset
     */
    public Map<Integer, Long> getTopicEndOffset(String topic) {
        Set<ObjectName> objs = getEndOffsetObjects(topic);
        if(objs == null) return null;
        Map<Integer, Long> map = new HashMap<Integer, Long>();
        for (ObjectName objName : objs) {
            int partitionId = getParId(objName);
            Object val = getAttribute(objName, "Value");
            if(val != null) map.put(partitionId, (Long) val);
        }
        return map;
    }

}
