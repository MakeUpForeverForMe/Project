ADD JAR hdfs:///user/hive/auxlib/HiveUDF-1.0-shaded.jar;

DROP FUNCTION IF EXISTS encrypt_aes;
DROP FUNCTION IF EXISTS decrypt_aes;
DROP FUNCTION IF EXISTS json_array_to_array;
DROP FUNCTION IF EXISTS datefmt;
DROP FUNCTION IF EXISTS age_birth;
DROP FUNCTION IF EXISTS age_idno;
DROP FUNCTION IF EXISTS sex_idno;
DROP FUNCTION IF EXISTS is_empty;
DROP FUNCTION IF EXISTS sha256;
DROP FUNCTION IF EXISTS date_max;
DROP FUNCTION IF EXISTS date_min;

CREATE FUNCTION encrypt_aes         AS 'com.weshare.udf.AesEncrypt'         USING JAR 'hdfs:///user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION decrypt_aes         AS 'com.weshare.udf.AesDecrypt'         USING JAR 'hdfs:///user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION json_array_to_array AS 'com.weshare.udf.AnalysisJsonArray'  USING JAR 'hdfs:///user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION datefmt             AS 'com.weshare.udf.DateFormat'         USING JAR 'hdfs:///user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION age_birth           AS 'com.weshare.udf.GetAgeOnBirthday'   USING JAR 'hdfs:///user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION age_idno            AS 'com.weshare.udf.GetAgeOnIdNo'       USING JAR 'hdfs:///user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION sex_idno            AS 'com.weshare.udf.GetSexOnIdNo'       USING JAR 'hdfs:///user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION is_empty            AS 'com.weshare.udf.IsEmpty'            USING JAR 'hdfs:///user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION sha256              AS 'com.weshare.udf.Sha256Salt'         USING JAR 'hdfs:///user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION date_max            AS 'com.weshare.udf.GetDateMax'         USING JAR 'hdfs:///user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION date_min            AS 'com.weshare.udf.GetDateMin'         USING JAR 'hdfs:///user/hive/auxlib/HiveUDF-1.0-shaded.jar';
