package com.weshare.udf;

import com.weshare.utils.AesPlus;
import com.weshare.utils.EmptyUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(
        name = "encrypt_aes",
        value = "_FUNC_(cleartext [, password]) - Input Cleartext [Password], Output Ciphertext",
        extended = "" +
                "Example:\n" +
                "  SELECT _FUNC_('18812345678'[,weshare666]);\n" +
                "    'AdDesv4O8b9QR5jIZ6hwgw=='"
)
public class AesEncrypt extends UDF {
    public String evaluate(String cleartext, String password) {
        if (EmptyUtil.isEmpty(cleartext)) return null;
        return new AesPlus().encrypt(cleartext, password);
    }

    public String evaluate(String cleartext) {
        return this.evaluate(cleartext, AesPlus.PASSWORD_WESHARE);
    }
}
