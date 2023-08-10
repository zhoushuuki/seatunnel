package org.apache.seatunnel.transform.encrypt.utils;

import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class AES {

	private final static String ENCODING = "UTF-8";
	public final static String KEY = "6QFD-PYH2-PPYF-C7RJ";
	private static Cipher enCipher;
	private static Cipher deCipher;

	/**
	* Description:  AES加密，不带key
	* @param data 要加密的数据
	* @author xf
	* @since 2022/8/16
	*/
	public static String encrypt(String data) throws Exception {
		return encrypt(data, KEY);
	}

	/**
	 * Description:  AES加密
	 * @param data 要加密的数据
	 * @param key 加密key
	 * @author xf
	 * @since 2022/8/16
	 */
	public static String encrypt(String data, String key) throws Exception {
		if (data == null) {
			return null;
		}
		setKey(getKey(key));
		return Base64.encodeBase64String(enCipher.doFinal(data
				.getBytes(ENCODING)));
	}

	/**
	* Description:  AES解密
	* @param data 要解密的数据
	* @author xf
	* @since 2022/8/16
	*/
	public static String decrypt(String data) throws Exception {
		return decrypt(data, KEY);
	}

	/**
	 * Description:  AES解密，带key
	 * @param data 要解密的数据
	 * @param key 加密key
	 * @author xf
	 * @since 2022/8/16
	 */
	public static String decrypt(String data, String key) throws Exception {
		if (data == null) {
			return null;
		}
		setKey(getKey(key));
		return new String(deCipher.doFinal(Base64.decodeBase64(data)));
	}


	private static void setKey(String key) throws Exception {
		SecretKeySpec secretKey = new SecretKeySpec(key.getBytes(), "AES");
		String transformation = "AES/ECB/PKCS5Padding";
		enCipher = Cipher.getInstance(transformation);
		deCipher = Cipher.getInstance(transformation);
		enCipher.init(Cipher.ENCRYPT_MODE, secretKey);
		deCipher.init(Cipher.DECRYPT_MODE, secretKey);
	}

	private static String getKey(String key) {
		key = key.replace("-", "");
		if (key.length() > 16) {
			key = key.substring(0, 16);
		}
		if (key.length() < 16) {
			StringBuilder sb = new StringBuilder();
			sb.append(key);
			for (int i = 0; i < 16 - key.length(); i++) {
				sb.append(" ");
			}
			key = sb.toString();
		}
		return key;
	}

    public static void main(String[] args) throws Exception {
        String encrypt = encrypt("asdasd", "wisesoft");
        System.out.println(encrypt);
        String decrypt = decrypt(encrypt, "wisesoft");
        System.out.println(decrypt);
    }
}
