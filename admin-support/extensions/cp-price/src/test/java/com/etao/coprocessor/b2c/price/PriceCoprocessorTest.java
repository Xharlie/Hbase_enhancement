/**
 * 
 */
package com.etao.coprocessor.b2c.price;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author dihong.wq
 * 
 */
public class PriceCoprocessorTest {
	private PriceCoprocessor coprocessor;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		coprocessor = new CrawlerPriceCoprocessor();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		coprocessor = null;
	}
	
	@Test
	public void testRemoveThousandSeparator() {
		String str = coprocessor.removeThousandSeparator("1,000，000");
		Assert.assertEquals("1000000", str);
		
		str = coprocessor.removeThousandSeparator("1000000");
		Assert.assertEquals("1000000", str);
	}
}
