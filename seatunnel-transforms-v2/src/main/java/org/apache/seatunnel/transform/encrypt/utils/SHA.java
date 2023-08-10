package org.apache.seatunnel.transform.encrypt.utils;

import org.apache.commons.codec.binary.Hex;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class SHA {
    public static final String TYPE = "SHA";


    /**
    * Description:   SHA加密-单项加密
    * @author xf
    * @since 2022/8/16
    */
    public static String encrypt(String info) throws NoSuchAlgorithmException{
        MessageDigest messageDigest;
        messageDigest = MessageDigest.getInstance(TYPE);
        byte[] hash = messageDigest.digest(info.getBytes(StandardCharsets.UTF_8));
        return Hex.encodeHexString(hash);
    }


    public static void main(String[] args) throws Exception {
        String encrypt = encrypt("123");
        System.out.println(encrypt);
    }

}
