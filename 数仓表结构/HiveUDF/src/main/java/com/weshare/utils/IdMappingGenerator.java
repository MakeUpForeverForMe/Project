package com.weshare.utils;

import org.apache.commons.lang3.StringUtils;

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
     * @param fieldType idNumber          // 身份证号
     *                  passport          // 护照编号
     *                  address           // 详细地址
     *                  userName          // 用户姓名
     *                  phone             // 手机号码
     *                  bankCard          // 银行卡号码或信用卡号码
     *                  imsi              // imsi、meid 手机唯一id
     *                  imei              // imei、idfa、idfv
     *                  plateNumber       // 车牌号码
     *                  houseNum          // 房产编号
     *                  frameNumber       // 车架号码
     *                  engineNumber      // 发动机号码
     *                  businessNumber    // 工商注册号
     *                  organizateCode    // 组织机构代码
     *                  taxpayerNumber    // 纳税人识别号
     *                  unifiedCreditCode // 统一信用代码
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
    public static String idGenerate(String key, int type) {
        if (EmptyUtil.isEmpty(key)) return null;
        return EncoderHandler.encodeBySHA256(type == 1 ? EncoderHandler.encodeBySHA256(key) + SALT : key);
    }
}
