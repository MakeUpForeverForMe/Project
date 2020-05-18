package com.travel.common;

public class Constants {

    public final static String ZK_IP = ConfigUtil.getConfig("hbase_connect");

    public final static String HBASE_OFFSET_STORE_TABLE ="hbase_offset_store";

    public final static String HBASE_OFFSET_ROWKEY = "offset_rowkey";

    public final static String HBASE_OFFSET_FAMILY_NAME = "f1";

    //经纬度栅格化半径(单位:米)
    public final static int GRID_LENGTH=100;
    //dev-cdh
    public final static String KAFKA_BOOTSTRAP_SERVERS = "KAFKA_BOOTSTRAP_SERVERS";
    //
    public final static String JEDIS_HOST="jedis.pool.host";
    public final static String JEDIS_PORT="jedis.pool.port";
    public final static String JEDIS_PASS="jedis.pool.password";


    public final static String CHENG_DU_GPS_TOPIC = "cheng_du_gps_topic";

    public final static String HAI_KOU_GPS_TOPIC = "hai_kou_gps_topic";

    public final static String VECHE = "veche";

    //存储订单gps轨迹信息
    public final static String HTAB_GPS = "HTAB_GPS";

    //海口订单信息表
    public final static String HTAB_HAIKOU_ORDER = "HTAB_HAIKOU_ORDER";

    //默认hbase表的列簇名
    public final static String DEFAULT_FAMILY = "f1";

    public final static String DEFAULT_DB_FAMILY= "MM";

    //成都市编码
    public final static String CITY_CODE_CHENG_DU = "075";

    //西安市城市编码
    public final static String CITY_CODE_XI_AN = "233";

    //海口
    public final static String CITY_CODE_HAI_KOU = "125";

    //订单数,在redis中作为hash结构的key名称
    public final static String ORDER_COUNT = "order_count";

    //人数统计,在redis中存储乘车人数实时统计的结果.
    public final static String PASSENGER_COUNT = "passenger_count";

    //实时订单
    public final static String REALTIME_ORDERS = "realtime_orders";

    //订单起始时间,将订单实体类进行序列化存入到redis中,用于判断订单是实时订单还是历史订单.
    public final static String ORDER_START_ENT_TIME = "order_start_end_time";

    public final static String VIRTUAL_STATION =  "VIRTUAL_STATIONS";

    //默认的region的个数
    public final static Integer DEFAULT_REGION_NUM = 8;


}
