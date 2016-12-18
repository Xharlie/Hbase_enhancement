/**
 * 
 */
package com.etao.hbase.coprocessor.trace.rule;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * trace rule for TraceCoprocessor
 * @author lingbo.zm
 *
 */
public class TraceRule {
	private static final Log LOG = LogFactory.getLog(TraceRule.class);

	private Column column;
	
	public TraceRule(String json) throws Exception {
		this.column = new Column(json);
	}
	
	public void apply(ObserverContext<RegionCoprocessorEnvironment> ctx, Put put) {
		Iterator<byte[]> familyIter = put.getFamilyCellMap().keySet().iterator();
	
		while (familyIter.hasNext()) {
			List<Cell> kvs = put.getFamilyCellMap().get(familyIter.next());
	    	
	    	Iterator<Cell> kvsIter = kvs.iterator();
	    	while (kvsIter.hasNext()) {
	    		Cell kv = kvsIter.next();
	    		
	    		try {
	    			if (!column.matches(kv)) {
		    			continue;
	    			}
	    			
	    			byte[] row = CellUtil.cloneRow(kv);
	    			byte[] family = CellUtil.cloneFamily(kv);
	    			byte[] qualifier = CellUtil.cloneQualifier(kv);
	    			
	    			Get get = new Get(row);
	    			get.addColumn(family, qualifier);
	    			
	    			Result result = ctx.getEnvironment().getRegion().get(get);
	            	if (result != null && !result.isEmpty()) {
	            		Cell  originalKV = result.getColumnLatestCell(family, qualifier);
	            	    if (originalKV == null) {
	            	    	continue;
	            	    }
	            		
	            		if (Bytes.equals(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength(), 
	            				originalKV.getValueArray(), originalKV.getValueOffset(), originalKV.getValueLength())) {
	                        kvsIter.remove();		
	            		}
	            	}
	    		} catch (Exception ex) {
	    			LOG.error("apply trace coprocessor rule error.", ex);
	    		}
	    	}
	    
	        if (kvs.isEmpty()) {
	            familyIter.remove();    	
	        }
		}
	}
	
	@Override
	public String toString() {
		return "column:" + this.column; 
	}
}
