package org.apache.seatunnel.transform.encrypt.utils;

import org.apache.commons.codec.binary.Hex;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

/**
 * Created by michael on 2017/11/14.
 */
public class MD5 {
	private static final char[] DIGITS = new char[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c',
			'd', 'e', 'f' };

	public static String encrypt(String str){
		try {
			MessageDigest msgDigest = MessageDigest.getInstance("MD5");
			msgDigest.update(str.getBytes(StandardCharsets.UTF_8));
			byte[] bytes = msgDigest.digest();
			return new String(encodeHex(bytes));
		} catch (Exception e) {
			return "";
		}
	}

	public static char[] encodeHex(byte[] data) {
		int l = data.length;
		char[] out = new char[l << 1];
		int i = 0;

		for (int j = 0; i < l; ++i) {
			out[j++] = DIGITS[(240 & data[i]) >>> 4];
			out[j++] = DIGITS[15 & data[i]];
		}

		return out;
	}

	/**
	* Description:  获取加盐的md5密码
	* @param password 用户密码
	* Author: xf
	* Date: 2022/5/17
	*/ 
	public static String getSaltMd5(String password){
		// 生成一个16位的随机数
		Random random = new Random();
		StringBuilder sBuilder = new StringBuilder(16);
		sBuilder.append(random.nextInt(99999999)).append(random.nextInt(99999999));
		int len = sBuilder.length();
		if (len < 16) {
			for (int i = 0; i < 16 - len; i++) {
				sBuilder.append("0");
			}
		}
		// 生成最终的加密盐
		String Salt = sBuilder.toString();
		password = md5Hex(password + Salt);
		char[] cs = new char[48];
		for (int i = 0; i < 48; i += 3) {
			cs[i] = password.charAt(i / 3 * 2);
			char c = Salt.charAt(i / 3);
			cs[i + 1] = c;
			cs[i + 2] = password.charAt(i / 3 * 2 + 1);
		}
		return String.valueOf(cs);
	}

	private static String md5Hex(String str) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] digest = md.digest(str.getBytes());
			return new String(new Hex().encode(digest));
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e);
			return "";
		}
	}


	/**
	 * 验证加盐后密码是否还相同
	 */
	public static boolean getSaltVerifyMD5(String password, String md5str) {
		char[] cs1 = new char[32];
		char[] cs2 = new char[16];
		for (int i = 0; i < 48; i += 3) {
			cs1[i / 3 * 2] = md5str.charAt(i);
			cs1[i / 3 * 2 + 1] = md5str.charAt(i + 2);
			cs2[i / 3] = md5str.charAt(i + 1);
		}
		String Salt = new String(cs2);
		return md5Hex(password + Salt).equals(String.valueOf(cs1));
	}

	public static void main(String[] args) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		System.out.println(MD5.encrypt("111111"));
	}
}
