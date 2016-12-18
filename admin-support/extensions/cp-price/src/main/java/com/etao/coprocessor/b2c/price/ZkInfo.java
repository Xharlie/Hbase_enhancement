/**
 * 
 */
package com.etao.coprocessor.b2c.price;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author dihong.wq
 * 
 */
public class ZkInfo {
	boolean isUpdate = false;
	long updateTimestamp;
	long time;
	float price;

	public ZkInfo(long time, float price) {
		this.time = time;
		this.price = price;
	}

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		long startTime = time >>> 32;
		long endTime = (time << 32) >>> 32;

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date start = new Date(startTime * 1000);
		Date end = new Date(endTime * 1000);

		buffer.append("[start=" + sdf.format(start) + ", end="
				+ sdf.format(end) + ", price=" + price + "]");

		return buffer.toString();
	}

}
