package com.weshare.utils;

/**
 * id mapping
 * 多个系统 按照统一规则生成ID
 * ETL时任意业务ID(手机号、imei、银行卡卡号、护照号、房产编号、车牌号、)如果能mapping到省份证号那么需要把该id对应的标签迁移到
 * 公司统一id的事实表里面去
 *
 * @author wushujia、ximing.wei
 */
public class IdMappingGenerator {
    private static final String SALT = "wsdSWedE34dcAKJHnLYGBSfgKfase2OU3dss";

    /**
     * 生成内部唯一ID
     *
     * @param encrypt   传入需要 Hash 的字段
     * @param fieldType @: default           // 默认值（当没有匹配成功时）
     *                  a: idNumber          // 身份证号
     *                  b: passport          // 护照编号
     *                  c: address           // 详细地址
     *                  d: userName          // 用户姓名
     *                  e: phone             // 手机号码
     *                  f: bankCard          // 银行卡号码或信用卡号码
     *                  g: imsi              // imsi、meid 手机唯一id
     *                  h: imei              // imei、idfa、idfv
     *                  i: plateNumber       // 车牌号码
     *                  j: houseNum          // 房产编号
     *                  k: frameNumber       // 车架号码
     *                  l: engineNumber      // 发动机号码
     *                  m: businessNumber    // 工商注册号
     *                  n: organizateCode    // 组织机构代码
     *                  o: taxpayerNumber    // 纳税人识别号
     *                  p: unifiedCreditCode // 统一信用代码
     * @param type      1:明文  2:SHA-256密文
     * @return 返回密文
     */
    public static String idGenerate(String encrypt, String fieldType, int type) {
        if (EmptyUtil.isEmpty(encrypt)) return null;
        String sign;
        switch (fieldType) {
            case "idNumber":          // 身份证号
                sign = "a_";
                break;
            case "passport":          // 护照编号
                sign = "b_";
                break;
            case "address":           // 详细地址
                sign = "c_";
                break;
            case "userName":          // 用户姓名
                sign = "d_";
                break;
            case "phone":             // 手机号码
                sign = "e_";
                break;
            case "bankCard":          // 银行卡号码或信用卡号码
                sign = "f_";
                break;
            case "imsi":              // imsi、meid 手机唯一id
                sign = "g_";
                break;
            case "imei":              // imei、idfa、idfv
                sign = "h_";
                break;
            case "plateNumber":       // 车牌号码
                sign = "i_";
                break;
            case "houseNum":          // 房产编号
                sign = "j_";
                break;
            case "frameNumber":       // 车架号码
                sign = "k_";
                break;
            case "engineNumber":      // 发动机号码
                sign = "l_";
                break;
            case "businessNumber":    // 工商注册号
                sign = "m_";
                break;
            case "organizateCode":    // 组织机构代码
                sign = "n_";
                break;
            case "taxpayerNumber":    // 纳税人识别号
                sign = "o_";
                break;
            case "unifiedCreditCode": // 统一信用代码
                sign = "p_";
                break;
            default:                  // 其他默认
                sign = "@_";
                break;
        }
        return sign + idGenerate(encrypt, type);
    }

    /**
     * 单业务id生成，返回hash值
     *
     * @param key  业务ID
     * @param type 1:明文  2:SHA-256密文
     * @return 返回密文
     */
    private static String idGenerate(String key, int type) {
        if (EmptyUtil.isEmpty(key)) return null;
        String newHashKey;
        if (type == 1) {
            newHashKey = EncoderHandler.sha256(key).toLowerCase();  // 统一用小写
        } else if (type == 2) {
            newHashKey = key.toLowerCase();  // 统一用小写
        } else {
            throw new IllegalArgumentException("The second arg " + type + " is not 1 or 2 !");
        }
        return EncoderHandler.sha256(newHashKey + SALT).toLowerCase();
    }
}
