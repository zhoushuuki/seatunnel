package org.apache.seatunnel.transform.encrypt.utils;

import org.apache.logging.log4j.util.Base64Util;

/**
 * ClassName: com.wisesoft.xodb.util.Base64Util
 * Name: Base64Util
 * Author: xf
 * Date: 2022/7/26 9:44
 * Description:
 */
public class Base64 {

    // 加密
    public static String encrypt(String str) {
        return Base64Util.encode(str);
    }


    // elasticsearch  es授权码的生成
    public static void main(String[] args) {
        System.out.println(encrypt("elastic:wisesoft123"));
    }

}
