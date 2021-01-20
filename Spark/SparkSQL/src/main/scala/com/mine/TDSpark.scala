package com.mine

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel


object TDSpark {

    //  def fun(dataset: Dataset[Row]): Unit = {
    //    dataset.joinWith(dataset, dataset.col(""))
    //  }


    def main(args: Array[String]): Unit = {

        System.setProperty("HADOOP_USER_NAME", "hadoop")

        val spark = SparkSession.builder().appName(this.getClass.getSimpleName.filter(!_.equals('$')))
                .master("local[*]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.shuffle.partitions", 1000)
                .config("spark.default.parallelism", 1000)
                .config("spark.cores.max", 20)
                .config("spark.executor.cores", 2)
                .config("spark.executor.memory", "10G")
                .config("fs.defaultFS", "hdfs://node5")
                .config("dfs.nameservices", "szdchdp310")
                .config("dfs.ha.namenodes.szdchdp310", "nn1,nn2")
                .config("dfs.namenode.rpc-address.szdchdp310.nn1", "sz-pg-dc-hadooptest-001.tendcloud.com:9820")
                .config("dfs.namenode.rpc-address.szdchdp310.nn2", "sz-pg-dc-hadooptest-002.tendcloud.com:9820")
                .config("dfs.client.failover.proxy.provider.szdchdp310", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
                .getOrCreate()

        import spark.implicits._

        val sc = spark.sparkContext

        spark.read.csv("").select("").groupBy("")

        // 第一张表      com.android.settings     1     6289460893461614016     2
        sc.textFile("hdfs://szdchdp310/user/winco_weshareholdings/app_info/dataByVersion_format.txt").toDF("value").createOrReplaceTempView("metaid_hash_tmp")
        spark.sql("select " +
                "split(value,'\t')[0] as pkgname, " +
                "split(value,'\t')[1] as metaid, " +
                "split(value,'\t')[2] as hash, " +
                "split(value,'\t')[3] as platform " +
                "from metaid_hash_tmp"
        ).createOrReplaceTempView("metaid_hash")



        // 第二张表      微信                     2       腾讯
        sc.textFile("hdfs://szdchdp310/user/winco_weshareholdings/app_info/appByVersion_format.txt").toDF("value").createOrReplaceTempView("appname_metaid_tmp")

        spark.sql("select " +
                "split(value,'\t')[0] as appnamecn, " +
                "split(value,'\t')[1] as metaid, " +
                "split(value,'\t')[2] as company " +
                "from appname_metaid_tmp"
        ).createOrReplaceTempView("appname_metaid")




        // 第三张表     1           T103010           T103000
        sc.textFile("hdfs://szdchdp310/user/winco_weshareholdings/app_info/AppClassification_format.txt").toDF("value").createOrReplaceTempView("metaid_code_tmp")

        spark.sql("select " +
                "split(value,'\t')[0] as metaid, " +
                "split(value,'\t')[1] as code, " +
                "split(value,'\t')[2] as parentcode " +
                "from metaid_code_tmp"
        ).createOrReplaceTempView("metaid_code")




        // 第四张表     T102010     T102000         应用商店        App Store         提供移动APP、移动游戏下载渠道的应用分发平台
        sc.textFile("hdfs://szdchdp310/user/winco_weshareholdings/app_info/StandardClassification_format").toDF("value").createOrReplaceTempView("code_codename_tmp")

        spark.sql("select " +
                "split(value,'\t')[0] as code, " +
                "split(value,'\t')[1] as parentcode, " +
                "split(value,'\t')[2] as name, " +
                "split(value,'\t')[3] as nameen, " +
                "split(value,'\t')[4] as description " +
                "from code_codename_tmp"
        ).createOrReplaceTempView("code_codename")





        // 第一张总表    -3598623770519591823        阿里通信        阿里巴巴          生活工具        系统工具
        spark.sql("select distinct " +
                "metaid_hash.hash as hash, " +
                "appname_metaid.appnamecn as appname, " +
                "pcode_tbl.name as pcode_name, " +
                "code_tbl.name as code_name " +
                "from metaid_hash " +
                "join appname_metaid on metaid_hash.metaid = appname_metaid.metaid " +
                "join metaid_code on metaid_hash.metaid = metaid_code.metaid " +
                "join code_codename as code_tbl on metaid_code.code = code_tbl.code " +
                "join code_codename as pcode_tbl on metaid_code.parentcode = pcode_tbl.parentcode"
        ).coalesce(1).repartition(1).write.mode(SaveMode.Overwrite).option("delimiter", "\t").csv("hdfs://szdchdp310/user/winco_weshareholdings/name_hash")

        sc.textFile("").coalesce(1).repartition(1)


        sc.textFile("hdfs://szdchdp310/user/winco_weshareholdings/name_hash").toDF("value").createOrReplaceTempView("name_hash_tmp")

        spark.sql("select " +
                "split(value,'\t')[0] as hash, " +
                "split(value,'\t')[1] as appname, " +
                "split(value,'\t')[3] as code_name, " +
                "split(value,'\t')[2] as pcode_name " +
                "from name_hash_tmp"
        ).persist(StorageLevel.DISK_ONLY).createOrReplaceTempView("name_hash")


        // 第五张表      3d549cf16a42b0a6fdbeb87e14628fd5e        6373029798449853408         蜜芽        3
        sc.textFile("hdfs://szdchdp310/user/winco_weshareholdings/tdid_hash/*").toDF("value").createOrReplaceTempView("tdid_hash_tmp")

        spark.sql("select " +
                "split(value,'\t')[0] as tdid, " +
                "split(value,'\t')[1] as hash, " +
                "split(value,'\t')[2] as appname " +
                "from tdid_hash_tmp"
        ).persist(StorageLevel.DISK_ONLY).createOrReplaceTempView("tdid_hash")


        // 联表
        spark.sql("select " +
                "tdid_hash.tdid, " +
                "name_hash.appname, " +
                "name_hash.code_name, " +
                "name_hash.pcode_name " +
                "from name_hash " +
                "join tdid_hash on name_hash.hash = tdid_hash.hash"
        ).write.mode(SaveMode.Overwrite).option("delimiter", "\t").csv("hdfs://szdchdp310/user/winco_weshareholdings/tdid_name")


        sc.textFile("hdfs://szdchdp310/user/winco_weshareholdings/tdid_name").toDF("value").persist(StorageLevel.DISK_ONLY).createOrReplaceTempView("tdid_name_tmp")

        spark.sql("select " +
                "split(value,'\t')[0] as tdid, " +
                "split(value,'\t')[1] as appname, " +
                "split(value,'\t')[3] as code_name, " +
                "split(value,'\t')[2] as pcode_name " +
                "from tdid_name_tmp"
        ).persist(StorageLevel.DISK_ONLY).createOrReplaceTempView("tdid_name")


        spark.sql("select * from tdid_name where appname like '%奢分期%'").show(10, truncate = false)


        // 联合计算 一、日活
        spark.sql("select " +
                "appname, " +
                "count(tdid) as total " +
                "from tdid_name " +
                "group by appname").repartition(1).write.mode(SaveMode.Overwrite).csv("hdfs://szdchdp310/user/winco_weshareholdings/daily_num")






        // 联合计算 二、重复率
        //    spark.sql("select " +
        //      "tdid1.appname as appmain, " +
        //      "d_num1.total as totalmian, " +
        //      "tdid2.appname as appmino, " +
        //      "d_num2.total as totalmino, " +
        //      "count(tdid1.tdid) as public, " +
        //      "count(tdid1.tdid)/d_num1.total as repeat_rate " +
        //      "from tdid_name as tdid1 " +
        //      "full join tdid_name as tdid2 on tdid1.appname != tdid2.appname and tdid1.tdid = tdid2.tdid " +
        //      "join daily_num as d_num1 on d_num1.appname = tdid1.appname " +
        //      "join daily_num as d_num2 on d_num2.appname = tdid2.appname " +
        //      "group by appmain,totalmian,appmino,totalmino"
        //    ).persist(StorageLevel.DISK_ONLY)
        //      .repartition(1)
        //      .write.mode(SaveMode.Overwrite)
        //      .csv("file:///home/winco_weshareholdings/repeat_rate")


        // 个别App计算
        sc.textFile("file:///home/winco_weshareholdings/appsum").toDF("value").createOrReplaceTempView("appsum_tmp")
        spark.sql("select " +
                "split(value,'\t')[0] as appname, " +
                "split(value,'\t')[1] as total " +
                "from appsum_tmp"
        ).createOrReplaceTempView("appsum_tmp2")

        spark.sql("select " +
                "appname, " +
                "total, " +
                "cast(pow(2,row_number() over(order by appname)) as bigint) as num " +
                "from appsum_tmp2"
        ).createOrReplaceTempView("appsum")


        spark.sql("select * from appsum").show(58, truncate = false)


        sc.textFile("file:///home/winco_weshareholdings/tdid_name").toDF("value").createOrReplaceTempView("tdid_name_tmp")
        spark.sql("select " +
                "split(value,'\t')[0] as tdid, " +
                "split(value,'\t')[1] as appname " +
                "from tdid_name_tmp"
        ).createOrReplaceTempView("tdid_name")

        spark.sql("select num,total,tdid,tdid_name.appname from appsum join tdid_name on appsum.appname = tdid_name.appname").createOrReplaceTempView("tdid_name_sum")

        spark.sql("select * from tdid_name_sum").show(false)

        spark.sql("select " +
                "public, " +
                "totalmian, " +
                "totalmino, " +
                "appmain, " +
                "appmino " +
                "from " +
                "( select " +
                "  row_number() over(partition by base order by base) as ch_base, " +
                "  appmain, " +
                "  totalmian, " +
                "  appmino, " +
                "  totalmino, " +
                "  public " +
                "  from " +
                "  ( select " +
                "    (tdid1.num + tdid2.num) as base, " +
                "    tdid1.appname as appmain, " +
                "    tdid1.total as totalmian, " +
                "    tdid2.appname as appmino, " +
                "    tdid2.total as totalmino, " +
                "    count(tdid1.tdid) as public " +
                "    from tdid_name_sum as tdid1 " +
                "    join tdid_name_sum as tdid2 " +
                "    on tdid1.appname != tdid2.appname and tdid1.tdid = tdid2.tdid " +
                "    group by base,appmain,totalmian,appmino,totalmino " +
                "  ) " +
                ") " +
                "where ch_base = 1 ").createOrReplaceTempView("repeat_rate")

        spark.sql("select * from repeat_rate").show(58, truncate = false)

    }
}
