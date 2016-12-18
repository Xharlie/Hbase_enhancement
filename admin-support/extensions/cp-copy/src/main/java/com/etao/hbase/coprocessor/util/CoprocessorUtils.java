/**
 * 
 */
package com.etao.hbase.coprocessor.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author yutian.xb
 *
 */
public class CoprocessorUtils {

    public static String toMD5String(byte[] bytes) {
	byte[] byteData = toMD5Bytes(bytes);
	if (null == byteData) {
	    return null;
	}

	StringBuffer sb = new StringBuffer();
	for (int i = 0; i < byteData.length; i++) {
	    sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
	}

	return sb.toString();
    }

    public static String toMD5String(String text) {
	if (null == text) {
	    return null;
	}

	return toMD5String(text.getBytes());
    }

    public static byte[] toMD5Bytes(byte[] bytes) {
	if (null == bytes) {
	    return null;
	}

	MessageDigest md = null;
	try {
	    md = MessageDigest.getInstance("MD5");
	} catch (NoSuchAlgorithmException e) {
	    e.printStackTrace();
	    return null;
	}

	md.update(bytes);
	return md.digest();
    }

    public static byte[] toMD5Bytes(String text) {
	if (null == text) {
	    return null;
	}

	return toMD5Bytes(text.getBytes());
    }

    public static void main(String[] args) {
	if (1 != args.length) {
	    System.err.println("Usage: exec <input_path>");
	    return;
	}

	BufferedReader reader = null;
	try {
	    reader = new BufferedReader(new FileReader(args[0]));
	    String line = null;
	    while (null != (line = reader.readLine())) {
		String md5Str = CoprocessorUtils.toMD5String(line);
		int hash = md5Str.hashCode();
		short partitionId = (short) ((0 > hash ? -hash : hash) % 100);
		System.out.println(partitionId + "\t" + line);
	    }
	} catch (IOException ioe) {
	    System.err.println("failed to read local file '" + args[1] + "'");
	} finally {
	    try {
		if (null != reader) {
		    reader.close();
		}
	    } catch (IOException e) {
	    }
	}
    }
}

