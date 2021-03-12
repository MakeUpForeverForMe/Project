package com.weshare.udf;

import com.weshare.utils.AesPlus;
import com.weshare.utils.EmptyUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(
        name = "decrypt_aes",
        value = "_FUNC_(ciphertext [, password]) - Input Ciphertext [Password], Output Cleartext",
        extended = "" +
                "Example:\n" +
                "  SELECT _FUNC_('AdDesv4O8b9QR5jIZ6hwgw=='[,weshare666]);\n" +
                "    '18812345678'"
)
public class AesDecrypt extends UDF {
    public String evaluate(String ciphertext, String password) {
        if (EmptyUtil.isEmpty(ciphertext)) return null;
        return new AesPlus().decrypt(ciphertext, password);
    }

    public String evaluate(String ciphertext) {
        return this.evaluate(ciphertext, AesPlus.PASSWORD_WESHARE);
    }
}
