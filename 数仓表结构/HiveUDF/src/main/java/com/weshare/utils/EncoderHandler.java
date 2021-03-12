package com.weshare.utils;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * blog www.micmiu.com
 * SHA1 和 MD5 加密算法
 *
 * @author wushujia、ximing.wei
 */
public class EncoderHandler {
    public static String encodeByMD5(String string) {
        return encode("MD5", string);
    }

    public static String encodeBySHA1(String string) {
        return encode("SHA1", string);
    }

    public static String encodeBySHA256(String string) {
        return encode("SHA-256", string);
    }

    public static String encodeBySHA512(String string) {
        return encode("SHA-512", string);
    }

    private static String encode(String algorithm, String string) {
        if (string == null) return null;
        final char[] HEX_DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(algorithm);
            messageDigest.update(string.getBytes("UTF-8"));
            byte[] digests = messageDigest.digest();
            StringBuilder stringBuilder = new StringBuilder(digests.length * 2);
            for (byte digest : digests) {
                stringBuilder.append(HEX_DIGITS[(digest >> 4) & 0x0f]);
                stringBuilder.append(HEX_DIGITS[digest & 0x0f]);
            }
            return stringBuilder.toString();
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
