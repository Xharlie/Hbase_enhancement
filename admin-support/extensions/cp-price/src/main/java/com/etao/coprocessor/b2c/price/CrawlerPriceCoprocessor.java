/**
 * 
 */
package com.etao.coprocessor.b2c.price;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import com.etao.util.NumUtils;

/**
 * @author dihong.wq
 * 
 */
public class CrawlerPriceCoprocessor extends PriceCoprocessor {

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,
			Put put, WALEdit edit, Durability durability) throws IOException {
		try{
			Region region = e.getEnvironment().getRegion();
	
			Delete delete = parserDelete(put);
			if (!delete.isEmpty()) {
				region.delete(delete);
			}
	
			Cell priceKv = parserPriceKv(put);
			Cell salesKv = parserSalesKv(put);
			Cell commentKv = parserCommentKv(put);
	
			if (priceKv == null && salesKv == null && commentKv == null) {
				return;
			}
	
			Get get = new Get(put.getRow());
			get.addColumn(Bytes.toBytes("trend"), trendPriceQualifier());
			get.addColumn(Bytes.toBytes("trend"), trendSalesQualifier());
			get.addColumn(Bytes.toBytes("trend"), trendCommentQualifier());
	
			Result result = region.get(get);
			KeyValue trendPriceKv = result.getColumnLatest(Bytes.toBytes("trend"),
					trendPriceQualifier());
			KeyValue trendSalesKv = result.getColumnLatest(Bytes.toBytes("trend"),
					trendSalesQualifier());
			KeyValue trendCommentKv = result.getColumnLatest(
					Bytes.toBytes("trend"), trendCommentQualifier());
	
			checkTrendPrice(put, priceKv, trendPriceKv);
			checkTrendSales(put, salesKv, trendSalesKv);
			checkTrendComment(put, commentKv, trendCommentKv);
		} catch (Throwable ex) {
			  logger.error("Price coprocessor preput exception: "+ex.getMessage(), ex);
			  throw new IOException(ex.getMessage(), ex);
	    }
	}

	private void checkTrendPrice(Put put, Cell priceKv, KeyValue trendPriceKv) {
		if (priceKv == null) {
			return;
		}

		long priceTs = priceKv.getTimestamp();
		// if the timestamp is not set in the put, replace it with the current
		// timestamp
		boolean priceTimeExist = false;
		if (priceTs != HConstants.LATEST_TIMESTAMP) {
			priceTimeExist = true;
		}
		
		String priceValue = Bytes.toString(priceKv.getValueArray(),
				priceKv.getValueOffset(), priceKv.getValueLength());
		priceValue = removeThousandSeparator(priceValue);
		if (!NumUtils.isNumeric(priceValue)) {
			return;
		}

		if (trendPriceKv != null) {
			long trendPriceTs = Bytes.toLong(trendPriceKv.getBuffer(),
					trendPriceKv.getTimestampOffset());
			String trendPriceValue = Bytes.toString(trendPriceKv.getBuffer(),
					trendPriceKv.getValueOffset(),
					trendPriceKv.getValueLength());

			float price = Float.parseFloat(priceValue);
			float trendPrice = -1;
			if (NumUtils.isNumeric(trendPriceValue)) {
				trendPrice = Float.parseFloat(trendPriceValue);
			}
			if (price != trendPrice && priceTs > trendPriceTs) {
				if (priceTimeExist) {
					put.add(Bytes.toBytes("trend"), trendPriceQualifier(),
							priceTs, Bytes.toBytes(priceValue));
				} else {
					put.add(Bytes.toBytes("trend"), trendPriceQualifier(),
							Bytes.toBytes(priceValue));
				}
			}
		} else {
			if (priceTimeExist) {
				put.add(Bytes.toBytes("trend"), trendPriceQualifier(), priceTs,
						Bytes.toBytes(priceValue));
			} else {
				put.add(Bytes.toBytes("trend"), trendPriceQualifier(),
						Bytes.toBytes(priceValue));
			}
		}
	}

	private void checkTrendSales(Put put, Cell salesKv, KeyValue trendSalesKv) {
		if (salesKv == null) {
			return;
		}

		long salesTs = salesKv.getTimestamp();
		if (salesTs == HConstants.LATEST_TIMESTAMP) {
			salesTs = System.currentTimeMillis();
		}
		String salesValue = Bytes.toString(salesKv.getValueArray(),
				salesKv.getValueOffset(), salesKv.getValueLength());
		salesValue = removeThousandSeparator(salesValue);
		if (!NumUtils.isNumeric(salesValue)) {
			return;
		}

		if (trendSalesKv != null) {
			long trendSalesTs = Bytes.toLong(trendSalesKv.getBuffer(),
					trendSalesKv.getTimestampOffset());
			String trendSalesValue = Bytes.toString(trendSalesKv.getBuffer(),
					trendSalesKv.getValueOffset(),
					trendSalesKv.getValueLength());
			if (!salesValue.equals(trendSalesValue) && salesTs > trendSalesTs) {
				put.add(Bytes.toBytes("trend"), trendSalesQualifier(), salesTs,
						Bytes.toBytes(salesValue));
			}
		} else {
			put.add(Bytes.toBytes("trend"), trendSalesQualifier(), salesTs,
					Bytes.toBytes(salesValue));
		}
	}

	private void checkTrendComment(Put put, Cell commentKv,
			KeyValue trendCommentKv) {
		if (commentKv == null) {
			return;
		}

		long commentTs = commentKv.getTimestamp();
		if (commentTs == HConstants.LATEST_TIMESTAMP) {
			commentTs = System.currentTimeMillis();
		}
		String commentValue = Bytes.toString(commentKv.getValueArray(),
				commentKv.getValueOffset(), commentKv.getValueLength());
		commentValue = removeThousandSeparator(commentValue);
		if (!NumUtils.isNumeric(commentValue)) {
			return;
		}

		// String nid = Md5Utils.getMd5String(HtmlUtil.getReverseURL(Bytes
		// .toString(put.getRow())));
		if (trendCommentKv != null) {
			long trendCommentTs = Bytes.toLong(trendCommentKv.getBuffer(),
					trendCommentKv.getTimestampOffset());
			String trendCommentValue = Bytes.toString(
					trendCommentKv.getBuffer(),
					trendCommentKv.getValueOffset(),
					trendCommentKv.getValueLength());
			if (!commentValue.equals(trendCommentValue)
					&& commentTs > trendCommentTs) {
				put.add(Bytes.toBytes("trend"), trendCommentQualifier(),
						commentTs, Bytes.toBytes(commentValue));
				// notifyHA3Engine(nid, COMMENT_FIELD, commentValue);
			}
		} else {
			put.add(Bytes.toBytes("trend"), trendCommentQualifier(), commentTs,
					Bytes.toBytes(commentValue));
			// notifyHA3Engine(nid, COMMENT_FIELD, commentValue);
		}
	}

	private Delete parserDelete(Put put) {
		Delete delete = new Delete(put.getRow());

		List<Cell> imagePriceKvList = put.get(Bytes.toBytes("image"),
				Bytes.toBytes("price"));
		List<Cell> jsPriceKvList = put.get(Bytes.toBytes("image"),
				Bytes.toBytes("js_price"));

		if (imagePriceKvList != null && imagePriceKvList.size() > 0) {
			Cell imagePriceKv = imagePriceKvList.get(0);
			String imagePriceStr = Bytes.toString(imagePriceKv.getValueArray(),
					imagePriceKv.getValueOffset(),
					imagePriceKv.getValueLength());
			if (NumUtils.isNumeric(imagePriceStr)) {
				delete.deleteColumn(Bytes.toBytes("image"),
						Bytes.toBytes("js_price"));
			}
		} else if (jsPriceKvList != null && jsPriceKvList.size() > 0) {
			Cell jsPriceKv = jsPriceKvList.get(0);
			String jsPriceStr = Bytes.toString(jsPriceKv.getValueArray(),
					jsPriceKv.getValueOffset(), jsPriceKv.getValueLength());
			if (NumUtils.isNumeric(jsPriceStr)) {
				delete.deleteColumn(Bytes.toBytes("image"),
						Bytes.toBytes("price"));
			}
		}

		return delete;
	}

	private Cell parserPriceKv(Put put) {
		Cell priceKv = null;

		List<Cell> specialPriceKvList = put.get(Bytes.toBytes("parser"),
				Bytes.toBytes("special_price"));
		List<Cell> nowPriceKvList = put.get(Bytes.toBytes("parser"),
				Bytes.toBytes("now_price"));

		if (specialPriceKvList != null && specialPriceKvList.size() > 0) {
			priceKv = specialPriceKvList.get(0);
		} else if (nowPriceKvList != null && nowPriceKvList.size() > 0) {
			priceKv = nowPriceKvList.get(0);
		}

		if (priceKv != null) {
			return priceKv;
		}

		List<Cell> imagePriceKvList = put.get(Bytes.toBytes("image"),
				Bytes.toBytes("price"));
		List<Cell> jsPriceKvList = put.get(Bytes.toBytes("image"),
				Bytes.toBytes("js_price"));

		if (imagePriceKvList != null && imagePriceKvList.size() > 0) {
			priceKv = imagePriceKvList.get(0);
		} else if (jsPriceKvList != null && jsPriceKvList.size() > 0) {
			priceKv = jsPriceKvList.get(0);
		}

		return priceKv;
	}

	private Cell parserSalesKv(Put put) {
		Cell salesKv = null;
		List<Cell> salesKvList = put.get(Bytes.toBytes("parser"),
				Bytes.toBytes("product_sales_volumn"));
		List<Cell> jsSalesKvList = put.get(Bytes.toBytes("parser"),
				Bytes.toBytes("js_product_sales_volumn"));
		if (jsSalesKvList != null && jsSalesKvList.size() > 0) {
			salesKv = jsSalesKvList.get(0);
		} else if (salesKvList != null && salesKvList.size() > 0) {
			salesKv = salesKvList.get(0);
		}

		return salesKv;
	}

	private Cell parserCommentKv(Put put) {
		Cell commentKv = null;
		List<Cell> commentKvList = put.get(Bytes.toBytes("parser"),
				Bytes.toBytes("comment_num"));
		List<Cell> jsCommentKvList = put.get(Bytes.toBytes("parser"),
				Bytes.toBytes("js_comment_num"));
		if (jsCommentKvList != null && jsCommentKvList.size() > 0) {
			commentKv = jsCommentKvList.get(0);
		} else if (commentKvList != null && commentKvList.size() > 0) {
			commentKv = commentKvList.get(0);
		}

		return commentKv;
	}
}
