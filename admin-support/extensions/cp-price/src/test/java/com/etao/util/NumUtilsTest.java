/**
 * 
 */
package com.etao.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author dihong.wq
 *
 */
public class NumUtilsTest {
	@Test
	public void testIsNumeric() {
		boolean ret = NumUtils.isNumeric("+abc");
		Assert.assertTrue(ret);
	}
}
