/**
 * 
 */
package com.etao.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author dihong.wq
 * 
 */
public class Md5Utils {
	private static byte[] getMd5Bytes(String key) {
		if (null == key) {
			return null;
		}

		try {
			MessageDigest algorithm = MessageDigest.getInstance("MD5");
			algorithm.reset();
			algorithm.update(key.getBytes("utf-8"));
			
			return algorithm.digest();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		return null;
	}

	public static String getMd5String(String key) {
		byte[] md5Bytes = getMd5Bytes(key);
		if (null == md5Bytes) {
			return "";
		}

		String md5String = String.valueOf(Bytes.toLong(md5Bytes));
		return md5String;
	}

	public static String getMd5HexString(String key) {
		byte[] md5Bytes = getMd5Bytes(key);
		if (null == md5Bytes) {
			return "";
		}

		StringBuffer hexSb = new StringBuffer();
		for (int i = 0; i < md5Bytes.length; ++i) {
			hexSb.append(Integer.toHexString(0xFF & md5Bytes[i]));
		}

		String hexString = hexSb.toString();
		return hexString;
	}
	
	public static void main(String[] args) {
		System.out.println(getMd5String("http://www.360buy.com/product/670019.html"));
	}
}
