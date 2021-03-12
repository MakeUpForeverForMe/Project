package com.weshare.utils;


import java.util.HashMap;
import java.util.Map;

/**
 * @author ximing.wei
 */
public class IdNumberUtil {
    private final static Map<Integer, Integer> FACTOR = new HashMap<Integer, Integer>() {{
        put(1, 7);
        put(2, 9);
        put(3, 10);
        put(4, 5);
        put(5, 8);
        put(6, 4);
        put(7, 2);
        put(8, 1);
        put(9, 6);
        put(10, 3);
        put(11, 7);
        put(12, 9);
        put(13, 10);
        put(14, 5);
        put(15, 8);
        put(16, 4);
        put(17, 2);
    }};
    private final static Map<Integer, String> CHECK_CODE = new HashMap<Integer, String>() {{
        put(0, "1");
        put(1, "0");
        put(2, "X");
        put(3, "9");
        put(4, "8");
        put(5, "7");
        put(6, "6");
        put(7, "5");
        put(8, "4");
        put(9, "3");
        put(10, "2");
    }};

    public static String len() {
        FACTOR.entrySet().forEach(entry -> System.out.println("FACTOR.entrySet()\t" + entry));
        CHECK_CODE.entrySet().forEach(entry -> System.out.println("CHECK_CODE.entrySet()\t" + entry));
        return "FACTOR.size():" + FACTOR.size() + "\t" + "CHECK_CODE.size():" + CHECK_CODE.size();
    }

    public static String get18IdNo(String idNo) throws Exception {
        if (EmptyUtil.isEmpty(idNo)) {
            return null;
        } else if (idNo.length() == 18) {
            return idNo;
        } else if (idNo.length() != 15) {
            return null;
            //            throw new Exception("The length of IdNumber is not 15 or 18 ! The length is " + idNo.length() + " and idNo is " + idNo);
        } else {
            int idNo18 = 0;
            idNo = idNo.substring(0, 6) + "19" + idNo.substring(6);

            for (int i = 0; i < idNo.length(); ++i) {
                idNo18 += Integer.valueOf(idNo.substring(i, i + 1)) * FACTOR.get(i + 1);
            }

            idNo = idNo + CHECK_CODE.get(idNo18 % 11);
            return idNo;
        }
    }
}
