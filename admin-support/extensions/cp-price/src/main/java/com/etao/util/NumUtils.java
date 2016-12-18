/**
 * 
 */
package com.etao.util;

import java.util.regex.Pattern;

/**
 * @author dihong.wq
 * 
 */
public class NumUtils {
	// private static Pattern PATTERN = Pattern.compile("^(-?\\d+)(\\.\\d+)?$");
	private static Pattern PATTERN = Pattern.compile("^([-,+]?\\d+)(\\.\\d+)?$", Pattern.MULTILINE);

	public static boolean isNumeric(String str) {
		return str == null ? false : PATTERN.matcher(str).matches();
	}
}
