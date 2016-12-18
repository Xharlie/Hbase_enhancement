/**
 * 
 */
package com.etao.coprocessor.b2c.price;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author dihong.wq
 * 
 */
public abstract class PriceCoprocessor extends BaseRegionObserver {
	protected Log logger = LogFactory.getLog(getClass());
	private static final String[] THOUSANDS_SEPARATOR = { ",", "，" };

	protected byte[] trendPriceQualifier() {
		return Bytes.toBytes("cp_price");
	}

	protected byte[] trendSalesQualifier() {
		return Bytes.toBytes("cp_product_sales_volumn");
	}

	protected byte[] trendCommentQualifier() {
		return Bytes.toBytes("cp_comment_num");
	}

	protected String removeThousandSeparator(String numStr) {
		String result = numStr;

		if (null != numStr) {
			for (String separator : THOUSANDS_SEPARATOR) {
				result = result.replaceAll(separator, "");
			}
		}

		return result;
	}

}
