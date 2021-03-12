package com.weshare.bigdata.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mouzwang on 2020-12-03 10:20
 */
public class KafkaJMXMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJMXMonitor.class);
    private static List<JMXConnection> conns = new ArrayList<>();

    public static boolean init(List<String> ipPortList) {
        for (String ipPort : ipPortList) {
            LOGGER.info("init jmx connection [{}]", ipPort);
            JMXConnection jmxConn = new JMXConnection(ipPort);
            boolean isConnect = jmxConn.init();
            if (!isConnect) {
                LOGGER.error("init jmx connection failed");
                return false;
            }
            conns.add(jmxConn);
        }
        return true;
    }

    /**
     *
     * @param topic
     * @return 获取相应topic的总发送量
     */
    public static long getMsgInCountPerSec(String topic) {
        long val = 0;
        for (JMXConnection conn : conns) {
            val += conn.getMsgInCountPerSec(topic);
        }
        return val;
    }

    /**
     *
     * @param topic
     * @return 获取相应topic的总TPS
     */
    public static double getMsgInTpsPerSec(String topic) {
        double dval = 0;
        for (JMXConnection conn : conns) {
            dval += conn.getMsgInTpsPerSec(topic);
        }
        return dval;
    }

    public static Map<Integer, Long> getEndOffset(String topic) {
        HashMap<Integer,Long> map = new HashMap<>();
        for (JMXConnection conn : conns) {
            Map<Integer, Long> topicEndOffset = conn.getTopicEndOffset(topic);
            if (topicEndOffset == null) {
                LOGGER.warn("get topic end offset return null,topic {}",topic);
                continue;
            }
            for (Integer partitionId : topicEndOffset.keySet()) {
                if (!map.containsKey(partitionId)
                        || (map.containsKey(partitionId) && topicEndOffset.get(partitionId) > map.get(partitionId))) {
                    map.put(partitionId, topicEndOffset.get(partitionId));
                }
            }
        }
        return map;
    }

    public static void main(String[] args) {
        ArrayList<String> ipPortList = new ArrayList<>();
        ipPortList.add("10.83.0.47:9393");
        ipPortList.add("10.83.0.123:9393");
        ipPortList.add("10.83.0.129:9393");
        String topic = "ATLAS_HOOK";
        init(ipPortList);
        System.out.println(getMsgInCountPerSec(topic));
        System.out.println(getMsgInTpsPerSec(topic));
        System.out.println(getEndOffset(topic));
    }
}
