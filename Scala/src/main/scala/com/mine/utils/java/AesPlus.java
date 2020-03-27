package com.mine.utils.java;


import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

/**
 * Aes工具类
 */
public class AesPlus {

  private static final Logger LOGGER = Logger.getLogger(AesPlus.class);

  private static final String KEY_ALGORITHM = "AES";
  private static final String MODE = "ECB";//默认的加密算法 字段
  private static final String PADDING = "pkcs5padding";//默认的加密算法 字段
  private static final String CIPHER_MODE = KEY_ALGORITHM + "/" + MODE + "/" + PADDING;
  private static final String CHARACTER = "UTF-8";//默认的加密算法 编码
  private static final String PASSWORD = "tencentabs123456";// 秘钥


  /**
   * AES 加密操作
   *
   * @param content 待加密内容
   * @return 返回Base64转码后的加密数据
   */
  public String encrypt(String content) {
    try {
      Cipher cipher = Cipher.getInstance(CIPHER_MODE);// 创建密码器
      byte[] byteContent = content.getBytes(CHARACTER);
      cipher.init(Cipher.ENCRYPT_MODE, getSecretKey(PASSWORD));// 初始化为加密模式的密码器
      byte[] result = cipher.doFinal(byteContent);// 加密
      return Base64.encodeBase64String(result);//通过Base64转码返回
    } catch (Exception ex) {
      LOGGER.error("AES Encryption Error", ex);
    }
    return null;
  }

  /**
   * AES 解密操作
   *
   * @param content
   * @return
   */
  public String decrypt(String content) {
    try {
      //实例化
      Cipher cipher = Cipher.getInstance(CIPHER_MODE);
      //使用密钥初始化，设置为解密模式
      cipher.init(Cipher.DECRYPT_MODE, getSecretKey(PASSWORD));
      //执行操作
      byte[] result = cipher.doFinal(Base64.decodeBase64(content));
      return new String(result, CHARACTER);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return null;
  }

  /**
   * 生成加密秘钥
   *
   * @return
   */
  private static SecretKeySpec getSecretKey(final String password) {
    //返回生成指定算法密钥生成器的 KeyGenerator 对象
    KeyGenerator kg = null;
    try {
      kg = KeyGenerator.getInstance(KEY_ALGORITHM);
      SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
      random.setSeed(password.getBytes());
      //AES 要求密钥长度为 128
      //kg.init(128, new SecureRandom(password.getBytes()));
      kg.init(128, random);

      //生成一个密钥
      SecretKey secretKey = kg.generateKey();

      return new SecretKeySpec(secretKey.getEncoded(), KEY_ALGORITHM);// 转换为AES专用密钥
    } catch (NoSuchAlgorithmException ex) {
      ex.printStackTrace();
    }

    return null;
  }
}
