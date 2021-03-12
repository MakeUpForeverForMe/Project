package com.weshare.udf;

import com.weshare.utils.IdMappingGenerator;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author ximing.wei
 */
@Description(
        name = "sha256",
        value = "_FUNC_(String source, String fieldType, int type)\n" +
                "  fieldType:{\n" +
                "    default           : @,\n" +
                "    idNumber          : a,\n" +
                "    passport          : b,\n" +
                "    address           : c,\n" +
                "    userName          : d,\n" +
                "    phone             : e,\n" +
                "    bankCard          : f,\n" +
                "    imsi              : g,\n" +
                "    imei              : h,\n" +
                "    plateNumber       : i,\n" +
                "    houseNum          : j,\n" +
                "    frameNumber       : k,\n" +
                "    engineNumber      : l,\n" +
                "    businessNumber    : m,\n" +
                "    organizateCode    : n,\n" +
                "    taxpayerNumber    : o,\n" +
                "    unifiedCreditCode : p,\n" +
                "  }\n" +
                "  type:{\n" +
                "    1 : Plaintext,\n" +
                "    2 : Ciphertext\n" +
                "  }",
        extended = "" +
                "Example:\n" +
                "  SELECT _FUNC_('18812345678', 'phone', 1) as t;\n" +
                "    e_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087\n" +
                "  SELECT _FUNC_('93492748c362146f8e48dde778cdb703ead158e42de292307baf24a9b4d4e61b', 'phone', 2) as t;\n" +
                "    e_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087"
)
public class Sha256Salt extends UDF {
    public String evaluate(String source, String fieldType, int type) {
        return IdMappingGenerator.idGenerate(source, fieldType, type);
    }
}
