/**
 * 
 */
package com.etao.coprocessor.b2c.price;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
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
public class TaobaoPriceCoprocessor extends PriceCoprocessor {
    private static int ZK_RATE_ORIGIN = 10000;
    private static long TIMERANGE_MAX = 0xffffffffL;

    // private static String ZK_GROUP_TAOJINBI = "3";
    private static String ZK_PLACEHOLDER = "-1";

    private static String MULTI_ZK_DELIMITER = " ";

    private static int IDX_NOT_EXISTED = -1;

    static Comparator<ZkInfo> comp = new Comparator<ZkInfo>() {
	@Override
	public int compare(ZkInfo zkInfo1, ZkInfo zkInfo2) {
	    long zkStartTime1 = zkInfo1.time >>> 32;
	    long zkStartTime2 = zkInfo2.time >>> 32;

	    int ret = 0;
	    if (zkStartTime1 < zkStartTime2) {
		ret = -1;
	    } else if (zkStartTime1 == zkStartTime2) {
		ret = 0;
	    } else {
		ret = 1;
	    }

	    return ret;
	}

    };

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
		try{
			List<Cell> reservePriceKvList = put.get(Bytes.toBytes("content"), Bytes.toBytes("reserve_price"));
			if (reservePriceKvList == null || reservePriceKvList.size() == 0) {
			    return;
			}
		
			Cell reservePriceKv = reservePriceKvList.get(0);
			String reservePriceValue = Bytes.toString(reservePriceKv.getValueArray(), reservePriceKv.getValueOffset(), reservePriceKv.getValueLength());
			reservePriceValue = removeThousandSeparator(reservePriceValue);
			if (!NumUtils.isNumeric(reservePriceValue)) {
			    return;
			}
			float reservePrice = Float.parseFloat(reservePriceValue);
		
			String[] zkTimes = parserZkTimes(put);
			String[] zkGroups = parserZkGroups(put);
			String[] zkFinalPrices = parserZkFinalPrices(put);
		
			if (zkTimes.length != zkFinalPrices.length || zkGroups.length != zkFinalPrices.length) {
			    return;
			}
		
			long currentTimeMillis = System.currentTimeMillis();
			long currentTimeSecs = currentTimeMillis / 1000;
		
			List<ZkInfo> zkInfoList = genZkInfoList(zkTimes, zkFinalPrices, zkGroups, currentTimeSecs);
			if (zkInfoList.isEmpty()) {
			    return;
			}
			analyze(zkInfoList);
			interpolate(currentTimeSecs, reservePrice, zkInfoList);
		
			ZkInfo[] zkInfos = zkInfoList.toArray(new ZkInfo[0]);
			Arrays.sort(zkInfos, comp);
		
			RegionCoprocessorEnvironment env = e.getEnvironment();
			Region region = env.getRegion();
		
			List<KeyValue> trendPriceKvList = findTrendPriceKvList(region, put, currentTimeMillis);
			Delete delete = analyzeDelete(put.getRow(), zkInfos, trendPriceKvList);
			if (!delete.isEmpty()) {
			    region.delete(delete);
			}
		
			KeyValue latestTrendPriceKv = findLatestTrendPriceKv(region, put, currentTimeSecs);
			boolean needCompare = (latestTrendPriceKv != null);
			for (ZkInfo zkInfo : zkInfos) {
			    float zkPrice = zkInfo.price;
			    long timestamp = zkInfo.isUpdate ? zkInfo.updateTimestamp : ((zkInfo.time >>> 32) * 1000 + 1);
			    if (needCompare) {
				String latestTrendPriceValue = Bytes.toString(latestTrendPriceKv.getBuffer(), latestTrendPriceKv.getValueOffset(), latestTrendPriceKv.getValueLength());
				if (NumUtils.isNumeric(latestTrendPriceValue)) {
				    if (zkPrice != Float.parseFloat(latestTrendPriceValue)) {
					put.add(Bytes.toBytes("trend"), trendPriceQualifier(), timestamp, Bytes.toBytes(String.valueOf(zkPrice)));
					needCompare = false;
				    }
				} else {
				    put.add(Bytes.toBytes("trend"), trendPriceQualifier(), timestamp, Bytes.toBytes(String.valueOf(zkPrice)));
				    needCompare = false;
				}
			    } else {
				put.add(Bytes.toBytes("trend"), trendPriceQualifier(), timestamp, Bytes.toBytes(String.valueOf(zkPrice)));
			    }
			}
		} catch (Throwable ex) {
			  logger.error("Price coprocessor preput exception: "+ex.getMessage(), ex);
			  throw new IOException(ex.getMessage(), ex);
	    }
    }

    /**
     * parser zk times from the put. if there's no zk_time, construct a zk time
     * (reserve_price_timestamp << 32 | 0xffffffffL)
     * 
     * @param put
     * @return
     */
    private String[] parserZkTimes(Put put) {
	String[] zkTimes = {};
	List<Cell> zkTimeKvList = put.get(Bytes.toBytes("content"), Bytes.toBytes("zk_time"));
	if (zkTimeKvList != null && zkTimeKvList.size() > 0) {
	    Cell zkTimeKv = zkTimeKvList.get(0);
	    String zkTimeValue = Bytes.toString(zkTimeKv.getValueArray(), zkTimeKv.getValueOffset(), zkTimeKv.getValueLength());
	    if (zkTimeValue.length() > 0) {
		zkTimes = zkTimeValue.split(MULTI_ZK_DELIMITER);
	    }
	}

	if (zkTimes.length == 0) {
	    Cell priceKv = put.get(Bytes.toBytes("content"), Bytes.toBytes("reserve_price")).get(0);
	    long priceTs = priceKv.getTimestamp();
	    long priceTsSecs = priceTs / 1000;
	    zkTimes = new String[] { String.valueOf((priceTsSecs << 32) | TIMERANGE_MAX) };
	}

	return zkTimes;
    }

    /**
     * parser zk rate from the put, if there's no zk_rate, construct a
     * zk_rate(10000)
     * 
     * @param put
     * @return
     */
    private String[] parserZkFinalPrices(Put put) {
	String[] zkFinalPrices = {};
	List<Cell> zkFinalPriceKvList = put.get(Bytes.toBytes("content"), Bytes.toBytes("zk_final_price"));
	if (zkFinalPriceKvList != null && zkFinalPriceKvList.size() > 0) {
	    Cell zkFinalPriceKv = zkFinalPriceKvList.get(0);
	    String zkFinalPriceValue = Bytes.toString(zkFinalPriceKv.getValueArray(), zkFinalPriceKv.getValueOffset(), zkFinalPriceKv.getValueLength());
	    if (zkFinalPriceValue.length() > 0) {
		zkFinalPrices = zkFinalPriceValue.split(MULTI_ZK_DELIMITER);
	    }
	}

	Cell priceKv = put.get(Bytes.toBytes("content"), Bytes.toBytes("reserve_price")).get(0);
	if (zkFinalPrices.length == 0) {
	    zkFinalPrices = new String[] { Bytes.toString(priceKv.getValue()) };
	}

	return zkFinalPrices;
    }

    /**
     * parser zk rate from the put, if there's no zk_rate, construct a
     * zk_rate(10000)
     * 
     * @param put
     * @return
     */
    private String[] parserZkRates(Put put) {
	String[] zkRates = {};
	List<Cell> zkRateKvList = put.get(Bytes.toBytes("content"), Bytes.toBytes("zk_rate"));
	if (zkRateKvList != null && zkRateKvList.size() > 0) {
	    Cell zkRateKv = zkRateKvList.get(0);
	    String zkRateValue = Bytes.toString(zkRateKv.getValueArray(), zkRateKv.getValueOffset(), zkRateKv.getValueLength());
	    if (zkRateValue.length() > 0) {
		zkRates = zkRateValue.split(MULTI_ZK_DELIMITER);
	    }
	}

	if (zkRates.length == 0) {
	    zkRates = new String[] { String.valueOf(ZK_RATE_ORIGIN) };
	}

	return zkRates;
    }

    /**
     * parser zk group from the put, if there's no zk_group, construct a
     * zk_group(1)
     * 
     * @param put
     * @return
     */
    private String[] parserZkGroups(Put put) {
	String[] zkGroups = {};
	List<Cell> zkGroupKvList = put.get(Bytes.toBytes("content"), Bytes.toBytes("zk_group"));
	if (zkGroupKvList != null && zkGroupKvList.size() > 0) {
	    Cell zkGroupKv = zkGroupKvList.get(0);
	    String zkGroupValue = Bytes.toString(zkGroupKv.getValueArray(), zkGroupKv.getValueOffset(), zkGroupKv.getValueLength());
	    if (zkGroupValue.length() > 0) {
		zkGroups = zkGroupValue.split(MULTI_ZK_DELIMITER);
	    }
	}

	if (zkGroups.length == 0) {
	    zkGroups = new String[] { ZK_PLACEHOLDER };
	}

	return zkGroups;
    }

    /**
     * find trend price list from the specified start time
     * 
     * @param region
     * @param put
     * @param startTime
     * @return
     */
    private List<KeyValue> findTrendPriceKvList(Region region, Put put, long startTime) {
	Get get = new Get(put.getRow());
	get.addColumn(Bytes.toBytes("trend"), trendPriceQualifier());

	List<KeyValue> trendPriceKvList = new ArrayList<KeyValue>();
	try {
	    get.setTimeRange(startTime, TIMERANGE_MAX * 1000);
	    get.setMaxVersions(65535);

	    Result result = region.get(get);
	    trendPriceKvList = result.getColumn(Bytes.toBytes("trend"), trendPriceQualifier());
	} catch (IOException ie) {
	    logger.warn("", ie);
	}

	return trendPriceKvList;
    }

    private KeyValue findLatestTrendPriceKv(Region region, Put put, long endTimeInSecs) {
	Get get = new Get(put.getRow());
	get.addColumn(Bytes.toBytes("trend"), trendPriceQualifier());

	try {
	    get.setTimeRange(0L, endTimeInSecs * 1000);

	    Result result = region.get(get);
	    return result.getColumnLatest(Bytes.toBytes("trend"), trendPriceQualifier());
	} catch (IOException ie) {
	    logger.warn("read exceptions occurred", ie);
	}

	return null;
    }

    private List<ZkInfo> genZkInfoList(String[] zkTimes, String[] zkFinalPrices, String[] zkGroups, long startTime) {
	List<ZkInfo> zkInfoList = new LinkedList<ZkInfo>();
	for (int i = 0; i < zkTimes.length; i++) {
	    try {
		long zkTime = Long.parseLong(zkTimes[i]);
		long zkEndTime = (zkTime << 32) >>> 32;
		if (zkEndTime < startTime) { // filter the timeout zk
		    continue;
		}

		long zkStartTime = zkTime >>> 32;
		if (zkStartTime < startTime) {
		    zkTime = (startTime << 32) | zkEndTime;
		}

		zkInfoList.add(new ZkInfo(zkTime, Float.parseFloat(zkFinalPrices[i])));
	    } catch (NumberFormatException nfe) {
		logger.warn("failed to parser ZK info, {ZkTimes=" + Arrays.asList(zkTimes) + ", ZkRates=" + Arrays.asList(zkFinalPrices) + "}", nfe);
		continue;
	    }
	}

	return zkInfoList;
    }

    /**
     * remove the overlapped zk_time
     * 
     * @param zkInfoList
     */
    private void analyze(List<ZkInfo> zkInfoList) {
	for (int i = 0; i < zkInfoList.size() - 1; i++) {
	    long zkTimeHighPriority = zkInfoList.get(i).time;
	    long zkStartTimeHighPriority = zkTimeHighPriority >>> 32;
	    long zkEndTimeHighPriority = (zkTimeHighPriority << 32) >>> 32;

	    for (int j = i + 1; j < zkInfoList.size(); j++) {
		long zkTimeLowPriority = zkInfoList.get(j).time;
		long zkStartTimeLowPriority = zkTimeLowPriority >>> 32;
		long zkEndTimeLowPriority = (zkTimeLowPriority << 32) >>> 32;

		// no overlap, continue
		if (zkEndTimeHighPriority <= zkStartTimeLowPriority || zkStartTimeHighPriority >= zkEndTimeLowPriority) {
		    continue;
		}

		if (zkStartTimeHighPriority <= zkStartTimeLowPriority && zkEndTimeHighPriority > zkStartTimeLowPriority) {
		    if (zkEndTimeHighPriority < zkEndTimeLowPriority) {
			zkInfoList.get(j).time = (zkEndTimeHighPriority << 32) | zkEndTimeLowPriority;
		    } else { // low priority time range is covered by the high
			     // priority time range
			zkInfoList.remove(j);
			j--;
		    }
		} else {
		    if (zkEndTimeHighPriority < zkEndTimeLowPriority) {
			zkInfoList.get(j).time = (zkStartTimeLowPriority << 32) | zkStartTimeHighPriority;
			long time = (zkEndTimeHighPriority << 32) | zkEndTimeLowPriority;
			float price = zkInfoList.get(j).price;
			zkInfoList.add(j + 1, new ZkInfo(time, price));
			// i++;
			j++;
		    } else {
			zkInfoList.get(j).time = (zkStartTimeLowPriority << 32) | zkStartTimeHighPriority;
		    }
		}
	    }
	}
    }

    /**
     * interpolate original price time range
     * 
     * @param zkInfoList
     *            must be processed by 'analyze' first
     */
    private void interpolate(long startTime, float reservePrice, List<ZkInfo> zkInfoList) {
	ZkInfo[] zkInfos = zkInfoList.toArray(new ZkInfo[0]);
	Arrays.sort(zkInfos, comp);

	List<ZkInfo> slopZkInfoList = new LinkedList<ZkInfo>();
	if (zkInfos.length > 0) {
	    long minZkStartTime = (zkInfos[0].time) >>> 32;
	    long maxZkEndTime = ((zkInfos[zkInfos.length - 1].time) << 32) >>> 32;
	    if (minZkStartTime > startTime) {
		slopZkInfoList.add(new ZkInfo((startTime << 32) | minZkStartTime, reservePrice));
	    }

	    if (maxZkEndTime < TIMERANGE_MAX) {
		slopZkInfoList.add(new ZkInfo((maxZkEndTime << 32) | TIMERANGE_MAX, reservePrice));
	    }
	}

	for (int i = 0; i < zkInfos.length - 1; i++) {
	    long zkEndTimeHighPriority = (zkInfos[i].time << 32) >>> 32;
	    long zkStartTimeLowPriority = zkInfos[i + 1].time >>> 32;
	    if (zkEndTimeHighPriority < zkStartTimeLowPriority) {
		slopZkInfoList.add(new ZkInfo((zkEndTimeHighPriority << 32) | zkStartTimeLowPriority, reservePrice));
	    }
	}
	zkInfoList.addAll(slopZkInfoList);
    }

    /**
     * analyze deletes by diff the 'zkInfos' and 'trendPriceKvList' by ts
     * 
     * @param rowkey
     * @param zkInfos
     * @param trendPriceKvList
     * @return
     */
    private Delete analyzeDelete(byte[] rowkey, ZkInfo[] zkInfos, List<KeyValue> trendPriceKvList) {
	Delete delete = new Delete(rowkey);
	for (KeyValue trendPriceKv : trendPriceKvList) {
	    long ts = Bytes.toLong(trendPriceKv.getBuffer(), trendPriceKv.getTimestampOffset());
	    int idx = findZkInfo(ts / 1000, zkInfos);
	    if (IDX_NOT_EXISTED == idx) {
		delete.deleteColumn(Bytes.toBytes("trend"), trendPriceQualifier(), ts);
	    } else {
		zkInfos[idx].isUpdate = true;
		zkInfos[idx].updateTimestamp = ts;
	    }
	}

	return delete;
    }

    private int findZkInfo(long tsInSecs, ZkInfo[] zkInfos) {
	for (int i = 0; i < zkInfos.length; i++) {
	    long startTime = zkInfos[i].time >>> 32;
	    if (tsInSecs == startTime) {
		return i;
	    }
	}

	return IDX_NOT_EXISTED;
    }

}
