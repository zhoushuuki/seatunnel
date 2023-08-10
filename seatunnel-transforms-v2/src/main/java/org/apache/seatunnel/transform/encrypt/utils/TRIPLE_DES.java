package org.apache.seatunnel.transform.encrypt.utils;

import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESedeKeySpec;
import javax.crypto.spec.IvParameterSpec;
import java.security.Key;

/**
 * description： ThreeDESUtil
 * 对des算法的密钥长度较短以及迭代次数偏少问题做了相应改进，提高了安全强度。
 * 不过desede算法处理速度较慢，密钥计算时间较长，加密效率不高问题使得对称加密算法的发展不容乐观。
 */

public class TRIPLE_DES {
    /**
     * 定义 加密算法,可用 DES,DESede,Blowfish
     */
    private static final String Algorithm = "DESede";

    /**
     * 算法/模式/补码方式
     */
    public static final String ALGORITHM_DES = "DESede/CBC/PKCS5Padding";

    // 加解密统一使用的编码方式
    private final static String ENCODING = "utf-8";

    public final static String KEY = "6QFD-PYH2-PPYF-C7RJ";
    public final static String iv = "01234567";


    public static String encrypt(String data) throws Exception {
        String key = MD5.encrypt(KEY);
        return encrypt(data, key, iv);
    }

    public static String decrypt(String data) throws Exception {
        String key = MD5.encrypt(KEY);
        return decrypt(data, key, iv);
    }

    /**
     * Description:  3des加密
     *
     * @param data   明文
     * @param keystr 密钥
     * @param iv     向量
     * @author xf
     * @since 2022/8/17
     */
    public static String encrypt(String data, String keystr, String iv) throws Exception {
        Key desKey;
        DESedeKeySpec spec = new DESedeKeySpec(keystr.getBytes(ENCODING));
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(Algorithm);
        desKey = keyFactory.generateSecret(spec);

        Cipher cipher = Cipher.getInstance(ALGORITHM_DES);
        IvParameterSpec ips = new IvParameterSpec(iv.getBytes());
        cipher.init(Cipher.ENCRYPT_MODE, desKey, ips);
        byte[] encryptData = cipher.doFinal(data.getBytes());
        return Base64.encodeBase64String(encryptData);
    }

    /**
     * Description:3des解密
     *
     * @param data   密文
     * @param keyStr 密钥
     * @param iv     向量
     * @author xf
     * @since 2022/8/17
     */
    public static String decrypt(String data, String keyStr, String iv) throws Exception {
        Key desKey;
        DESedeKeySpec spec = new DESedeKeySpec(keyStr.getBytes());
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(Algorithm);
        desKey = keyFactory.generateSecret(spec);
        Cipher cipher = Cipher.getInstance(ALGORITHM_DES);
        IvParameterSpec ips = new IvParameterSpec(iv.getBytes());
        cipher.init(Cipher.DECRYPT_MODE, desKey, ips);
        byte[] decryptData = cipher.doFinal(Base64.decodeBase64(data));
        return new String(decryptData, ENCODING);
    }

    public static void main(String[] args) throws Exception {
        System.out.println(TRIPLE_DES.encrypt("cc"));
        System.out.println(TRIPLE_DES.decrypt("2NQu4HQ21bg="));
    }
}