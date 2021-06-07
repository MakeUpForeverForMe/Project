package com.weshare.bigdata.util;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.MessageDigest;
import java.util.Arrays;

public class AESPlus {

    @SuppressWarnings("all")
    public static byte[] encrypt(String strKey, String strIn) throws Exception {
        SecretKeySpec skeySpec = getKey(strKey);
        Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
        String ivs = "F0E1D2C3B4A5968778695A4B3C2D1E0F";
        byte[] ivb  = hexStr2Bytes(ivs);
        IvParameterSpec iv = new IvParameterSpec(ivb);
        cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);
        byte[] strInBytes = strIn.getBytes();
        int size = (strInBytes.length+15)/16 * 16;
        byte[] newIn = new byte[size];
        for(int i=0;i<size;i++){
            if(i<strInBytes.length) {
                newIn[i] = strInBytes[i];
            }
            else {
                newIn[i] = 0;
            }
        }
        return cipher.doFinal(newIn);
    }

    @SuppressWarnings("all")
    public static byte[] decrypt(String strKey, String strIn) throws Exception {
        byte[] strInBytes = parseStr(strIn);
        SecretKeySpec skeySpec = getKey(strKey);
        Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
        String ivs = "F0E1D2C3B4A5968778695A4B3C2D1E0F";
        byte[] ivb  = hexStr2Bytes(ivs);
        IvParameterSpec iv = new IvParameterSpec(ivb);
        cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);
        byte[] encrypted = cipher.doFinal(strInBytes);
        int alen=encrypted.length;
        for(int i=0;i<encrypted.length;i++){
            if(encrypted[i]==0){
                alen=i;
                break;
            }
        }
        if (alen == encrypted.length)
        {
            return encrypted;
        }
        return Arrays.copyOf(encrypted,alen);
    }

    @SuppressWarnings("all")
    public static String getStr(byte[] md){
        char hexDigits[]={'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
        int j = md.length;
        char str[] = new char[j * 2];
        int k = 0;
        for (byte byte0 : md) {
            str[k++] = hexDigits[byte0 >>> 4 & 0xf];
            str[k++] = hexDigits[byte0 & 0xf];
        }
        return new String(str);
    }

    private static byte[] parseStr(String hexString){
        String hex ="0123456789ABCDEF";
        if (hexString == null || hexString.equals("")) {
            return null;
        }
        hexString = hexString.toUpperCase();
        int length = hexString.length() / 2;
        char[] hexChars = hexString.toCharArray();
        byte[] d = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            d[i] = (byte) (hex.indexOf((hexChars[pos])) << 4 | hex.indexOf((hexChars[pos + 1])));
        }
        return d;
    }

    private static byte uniteBytes(String src0, String src1) {
        byte b0 = Byte.decode("0x" + src0);
        b0 = (byte) (b0 << 4);
        byte b1 = Byte.decode("0x" + src1);
        return (byte) (b0 | b1);
    }

    /**
     * bytes转换成十六进制字符串
     */
    private static byte[] hexStr2Bytes(String src) {
        int m , n ;
        int l = src.length() / 2;
        byte[] ret = new byte[l];
        for (int i = 0; i < l; i++) {
            m = i * 2 + 1;
            n = m + 1;
            ret[i] = uniteBytes(src.substring(i * 2, m), src.substring(m, n));
        }
        return ret;
    }

    private static SecretKeySpec getKey(String strKey) throws Exception {
        byte[] arrBTmp = strKey.getBytes();
        byte[] arrB = new byte[16]; // 创建一个空的16位字节数组（默认值为0）

        for (int i = 0; i < arrBTmp.length && i < arrB.length; i++) {
            arrB[i] = arrBTmp[i];
        }
        return new SecretKeySpec(arrB, "AES");
    }

    /**
     * ABS字段加密
     * @param value 字段
     * @return 结果
     * @throws Exception 抛出异常
     */
    public static String absEncrypt(String key, String value) throws Exception{
        byte[] codE;
        codE = encrypt(key, value.trim());
        return getStr(codE);
    }

    /**
     * ABS字段解密
     * @param value 字段
     * @return 结果
     * @throws Exception 抛出异常
     */
    public static String absDecrypt(String key, String value) throws Exception{
        byte[] codE = decrypt(key,value);
        return new String(codE,"UTF-8");
    }


    public static String getSHA256StrJava(String str){
        MessageDigest messageDigest;
        String encodeStr = "";
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.update(str.getBytes("UTF-8"));
            encodeStr = byte2Hex(messageDigest.digest());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return encodeStr;
    }

    private static String byte2Hex(byte[] bytes){
        StringBuilder stringBuffer = new StringBuilder();
        String temp = null;
        for (byte aByte : bytes) {
            temp = Integer.toHexString(aByte & 0xFF);
            if (temp.length() == 1) {
                stringBuffer.append("0");
            }
            stringBuffer.append(temp);
        }
        return stringBuffer.toString();
    }

    public static void main(String[] args) throws Exception {
        String Code = "530111197709112641";
        String key = "ABSYunBusi2017@clound";
        byte[] codE;
        byte[] codD;

        codE = AESPlus.encrypt(key, Code);
//        String scodE = getStr(codE);
        String scodE = "D14D6B85E7E403C8760C94E2C43F12534304E1E8913E1318D572F653FCAC76029AE620DF86C15D23D3DC79B8AF9E6C55A521A9D8C3794A20C7BEE660253A9D5B98D42BA41B2F4CF6A3C7EBB045419E3B";

        codD = AESPlus.decrypt("8Wgnk9xqg6ce1Cea", "C2F0FA9A89D7266BD8495BF1B567D42D80E6F434A7E361A15C78BAD08B06A45D8C69FE6754DC8982E2FC30D9AFF0EEB6C99B3DC43C873FEE7EE2B2A2CABE2A4B2327A0EAE3B7266EC7FA1B5E95F6AC3B82CE41C540C7AC5F501C383A8242AF7DE5E391462E4CB55B765977C4E3B17F99B39B0EA65D05ECEFBB08770C71AEEBF5790752643CAD9729F7EAF353AAFC5EE6");

        System.out.println("原文：" + Code);
        System.out.println("密钥：" + key);
        System.out.println("密文：" + scodE);
        System.out.println("解密：" + new String(codD));
    }
}