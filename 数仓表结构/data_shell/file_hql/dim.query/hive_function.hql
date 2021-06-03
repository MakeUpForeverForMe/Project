set hivevar:uri=cosn://bigdatacenter-sit-1253824322;
set hivevar:uri=cosn://bigdata-center-prod-1253824322;

ADD JAR ${uri}/user/auxlib/HiveUDF-1.0-shaded.jar;

DROP FUNCTION IF EXISTS encrypt_aes;
DROP FUNCTION IF EXISTS decrypt_aes;
DROP FUNCTION IF EXISTS datefmt;
DROP FUNCTION IF EXISTS age_birth;
DROP FUNCTION IF EXISTS age_idno;
DROP FUNCTION IF EXISTS sex_idno;
DROP FUNCTION IF EXISTS sha256;
DROP FUNCTION IF EXISTS date_max;
DROP FUNCTION IF EXISTS date_min;
DROP FUNCTION IF EXISTS ptrim;
DROP FUNCTION IF EXISTS map_from_str;
DROP FUNCTION IF EXISTS json_array_to_array;
DROP FUNCTION IF EXISTS js2str;

DROP FUNCTION IF EXISTS is_empty;

CREATE FUNCTION encrypt_aes         AS 'com.weshare.udf.AesEncrypt'                     USING JAR '${uri}/user/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION decrypt_aes         AS 'com.weshare.udf.AesDecrypt'                     USING JAR '${uri}/user/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION datefmt             AS 'com.weshare.udf.DateFormat'                     USING JAR '${uri}/user/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION age_birth           AS 'com.weshare.udf.GetAgeOnBirthday'               USING JAR '${uri}/user/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION age_idno            AS 'com.weshare.udf.GetAgeOnIdNo'                   USING JAR '${uri}/user/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION sex_idno            AS 'com.weshare.udf.GetSexOnIdNo'                   USING JAR '${uri}/user/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION sha256              AS 'com.weshare.udf.Sha256Salt'                     USING JAR '${uri}/user/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION date_max            AS 'com.weshare.udf.GetDateMax'                     USING JAR '${uri}/user/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION date_min            AS 'com.weshare.udf.GetDateMin'                     USING JAR '${uri}/user/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION ptrim               AS 'com.weshare.udf.TrimPlus'                       USING JAR '${uri}/user/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION map_from_str        AS 'com.weshare.udf.AnalysisStringToJson'           USING JAR '${uri}/user/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION json_array_to_array AS 'com.weshare.udf.AnalysisJsonArray'              USING JAR '${uri}/user/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION js2str              AS 'com.weshare.udf.JsonString2StringUDF'           USING JAR '${uri}/user/auxlib/HiveUDF-1.0-shaded.jar';

CREATE FUNCTION is_empty            AS 'com.weshare.generic.IsEmptyGenericUDF'          USING JAR '${uri}/user/auxlib/HiveUDF-1.0-shaded.jar';

reload function; -- 多个 HiveServer 之间，需要同步元数据信息
