/**
 * 
 */
package com.etao.hbase.coprocessor.increment;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;

public class Replication3Coprocessor extends BaseRegionObserver {
    private static final Log LOG = LogFactory.getLog(Replication3Coprocessor.class);

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
    }

    @Override
    public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
	long curTime = System.currentTimeMillis();
	for (int i = 0; i < miniBatchOp.size(); ++i) {
	    if (miniBatchOp.getOperationStatus(i).getOperationStatusCode() != OperationStatusCode.SUCCESS) {
		continue;
	    }

	    Mutation mutation = miniBatchOp.getOperation(i);
	    if (mutation instanceof Put) {
		Put put = (Put) mutation;
		String rowName = Bytes.toString(put.getRow());
		Map<byte[], List<Cell>> map = put.getFamilyCellMap();
		for (Map.Entry<byte[], List<Cell>> e : map.entrySet()) {
		    String cfName = Bytes.toString(e.getKey());
		    for (Cell cell : e.getValue()) {
			String qName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
			String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
			LOG.debug(rowName + ":" + cfName + ":" + qName + "\t" + value + "\t" + curTime + "\t" + (curTime - Long.parseLong(value)));
		    }
		}
	    }
	}
    }
}
