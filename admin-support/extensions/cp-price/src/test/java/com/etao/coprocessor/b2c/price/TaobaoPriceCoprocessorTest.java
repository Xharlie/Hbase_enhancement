/**
 * 
 */
package com.etao.coprocessor.b2c.price;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author dihong.wq
 * 
 */
public class TaobaoPriceCoprocessorTest {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testAnalyze() {
		List<ZkInfo> zkInfoList = new LinkedList<ZkInfo>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		try {
			long startTime1 = sdf.parse("2012-05-01 08:00:00").getTime() / 1000;
			long endTime1 = sdf.parse("2012-05-01 12:00:00").getTime() / 1000;
			long time1 = startTime1 << 32 | endTime1;
			ZkInfo zkInfo1 = new ZkInfo(time1, 5000);
			zkInfoList.add(zkInfo1);

			long startTime2 = sdf.parse("2012-05-01 11:00:00").getTime() / 1000;
			long endTime2 = sdf.parse("2012-05-01 13:00:00").getTime() / 1000;
			long time2 = startTime2 << 32 | endTime2;
			ZkInfo zkInfo2 = new ZkInfo(time2, 6000);
			zkInfoList.add(zkInfo2);

			long startTime3 = sdf.parse("2012-05-01 14:00:00").getTime() / 1000;
			long endTime3 = sdf.parse("2012-05-01 18:00:00").getTime() / 1000;
			long time3 = startTime3 << 32 | endTime3;
			ZkInfo zkInfo3 = new ZkInfo(time3, 7000);
			zkInfoList.add(zkInfo3);

			long startTime4 = sdf.parse("2012-05-01 07:00:00").getTime() / 1000;
			long endTime4 = sdf.parse("2012-05-01 20:00:00").getTime() / 1000;
			long time4 = startTime4 << 32 | endTime4;
			ZkInfo zkInfo4 = new ZkInfo(time4, 8000);
			zkInfoList.add(zkInfo4);

			long startTime5 = sdf.parse("2012-05-01 09:00:00").getTime() / 1000;
			long endTime5 = sdf.parse("2012-05-01 10:00:00").getTime() / 1000;
			long time5 = startTime5 << 32 | endTime5;
			ZkInfo zkInfo5 = new ZkInfo(time5, 9000);
			zkInfoList.add(zkInfo5);

			long startTime6 = sdf.parse("2012-05-01 10:00:00").getTime() / 1000;
			long endTime6 = sdf.parse("2012-05-01 22:00:00").getTime() / 1000;
			long time6 = startTime6 << 32 | endTime6;
			ZkInfo zkInfo6 = new ZkInfo(time6, 9500);
			zkInfoList.add(zkInfo6);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		TaobaoPriceCoprocessor tpc = new TaobaoPriceCoprocessor();
		try {
			Method method = tpc.getClass().getDeclaredMethod("analyze",
					List.class);
			method.setAccessible(true);
			method.invoke(tpc, zkInfoList);
			for (ZkInfo zkInfo : zkInfoList) {
				System.out.println(zkInfo);
			}
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testInterpolate() {
		List<ZkInfo> zkInfoList = new LinkedList<ZkInfo>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		try {
			long startTime1 = sdf.parse("2012-05-01 08:00:00").getTime() / 1000;
			long endTime1 = sdf.parse("2012-05-01 10:00:00").getTime() / 1000;
			long time1 = startTime1 << 32 | endTime1;
			ZkInfo zkInfo1 = new ZkInfo(time1, 5000);
			zkInfoList.add(zkInfo1);

			long startTime2 = sdf.parse("2012-05-01 06:00:00").getTime() / 1000;
			long endTime2 = sdf.parse("2012-05-01 07:00:00").getTime() / 1000;
			long time2 = startTime2 << 32 | endTime2;
			ZkInfo zkInfo2 = new ZkInfo(time2, 6000);
			zkInfoList.add(zkInfo2);

			long startTime3 = sdf.parse("2012-05-01 12:00:00").getTime() / 1000;
			long endTime3 = sdf.parse("2012-05-01 14:00:00").getTime() / 1000;
			long time3 = startTime3 << 32 | endTime3;
			ZkInfo zkInfo3 = new ZkInfo(time3, 6000);
			zkInfoList.add(zkInfo3);

			long startTime4 = sdf.parse("2012-05-01 15:00:00").getTime() / 1000;
			long endTime4 = sdf.parse("2012-05-01 18:00:00").getTime() / 1000;
			long time4 = startTime4 << 32 | endTime4;
			ZkInfo zkInfo4 = new ZkInfo(time4, 7000);
			zkInfoList.add(zkInfo4);

			long startTime5 = sdf.parse("2012-05-01 20:00:00").getTime() / 1000;
			long endTime5 = sdf.parse("2012-05-01 22:00:00").getTime() / 1000;
			long time5 = startTime5 << 32 | endTime5;
			ZkInfo zkInfo5 = new ZkInfo(time5, 8000);
			zkInfoList.add(zkInfo5);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		TaobaoPriceCoprocessor tpc = new TaobaoPriceCoprocessor();
		try {
			Method method = tpc.getClass().getDeclaredMethod("interpolate",
					List.class);
			method.setAccessible(true);
			method.invoke(tpc, zkInfoList);
			for (ZkInfo zkInfo : zkInfoList) {
				System.out.println(zkInfo);
			}
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testParserZkTimes() {
		Put put = new Put(Bytes.toBytes("test-rowkey"));
		put.add(Bytes.toBytes("content"),
				Bytes.toBytes("zk_time"),
				Bytes.toBytes("4133735389080056692 4133735389080066692 4133735389080076692"));

		TaobaoPriceCoprocessor tpc = new TaobaoPriceCoprocessor();
		try {
			Method method = tpc.getClass().getDeclaredMethod("parserZkTimes",
					Put.class);
			method.setAccessible(true);
			String[] zkTimes = (String[]) method.invoke(tpc, put);
			Assert.assertEquals(3, zkTimes.length);
			Assert.assertArrayEquals(new String[] { "4133735389080056692",
					"4133735389080066692", "4133735389080076692" }, zkTimes);
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testParserZkRates() {
		Put put = new Put(Bytes.toBytes("test-rowkey"));
		put.add(Bytes.toBytes("content"), Bytes.toBytes("zk_rate"),
				Bytes.toBytes("7022 6500"));

		TaobaoPriceCoprocessor tpc = new TaobaoPriceCoprocessor();
		try {
			Method method = tpc.getClass().getDeclaredMethod("parserZkRates",
					Put.class);
			method.setAccessible(true);
			String[] zkRates = (String[]) method.invoke(tpc, put);
			Assert.assertEquals(2, zkRates.length);
			Assert.assertArrayEquals(new String[] { "7022", "6500" }, zkRates);
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testParserZkGroups() {
		Put put = new Put(Bytes.toBytes("test-rowkey"));
		put.add(Bytes.toBytes("content"), Bytes.toBytes("zk_group"),
				Bytes.toBytes("1 3 88"));

		TaobaoPriceCoprocessor tpc = new TaobaoPriceCoprocessor();
		try {
			Method method = tpc.getClass().getDeclaredMethod("parserZkGroups",
					Put.class);
			method.setAccessible(true);
			String[] zkGroups = (String[]) method.invoke(tpc, put);
			Assert.assertEquals(3, zkGroups.length);
			Assert.assertArrayEquals(new String[] { "1", "3", "88" }, zkGroups);
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGenZkInfoList() {
		String[] zkTimes = { "5721530930844691583", "5721530930944691583" };
		String[] zkRates = { "8735", "7735" };
		String[] zkGroups = { "3", "1" };

		TaobaoPriceCoprocessor tpc = new TaobaoPriceCoprocessor();
		try {
			Method method = tpc.getClass().getDeclaredMethod("genZkInfoList",
					String[].class, String[].class, String[].class, long.class);
			method.setAccessible(true);
			List<ZkInfo> zkInfos = (List<ZkInfo>) method.invoke(tpc, zkTimes,
					zkRates, zkGroups, System.currentTimeMillis() / 1000);
			Assert.assertEquals(1, zkInfos.size());
			for (ZkInfo zkInfo : zkInfos) {
				System.out.println(zkInfo);
			}
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

	}
}
