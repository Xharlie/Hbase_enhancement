/**
 * 
 */
package com.etao.hbase.coprocessor.copy.rule;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;

import com.etao.hbase.coprocessor.copy.common.CopyCoprocessorConstants;

/**
 * action for CopyCoprocessor rule
 * @author lingbo.zm
 *
 */
public class Action {
	private static final Log LOG = LogFactory.getLog(Action.class);
	
	private String target;
	
	public Action(JSONObject json) throws Exception {
    	target = json.getString(CopyCoprocessorConstants.JSON_ACTION_TARGET);
	}
	
	public Cell execute(Cell kv) {
		try {
			byte[][] column = KeyValue.parseColumn(Bytes.toBytes(target));
			
			byte[] targetFamily = column[0];
			byte[] targetQualifier = column[1];
			
			if (Bytes.equals(CopyCoprocessorConstants.JSON_PLACEHOLDER_COLUMN_BYTES, column[1])) {
				byte[] family = CellUtil.cloneFamily(kv);
				byte[] qualifier = CellUtil.cloneQualifier(kv);
				
				targetQualifier = KeyValue.makeColumn(family, qualifier);
			} else if (Bytes.equals(CopyCoprocessorConstants.JSON_PLACEHOLDER_QUALIFIER_BYTES, column[1])) {
				targetQualifier = CellUtil.cloneQualifier(kv);
			}
			
			long targetTimestamp = kv.getTimestamp();
			if (targetTimestamp == HConstants.LATEST_TIMESTAMP) {
                targetTimestamp = System.currentTimeMillis();
			}
			
			byte[] targetRow = CellUtil.cloneRow(kv);
		    byte[] targetValue = CellUtil.cloneValue(kv);
			
		    return new KeyValue(targetRow, targetFamily, targetQualifier, targetTimestamp, targetValue);
		} catch (Exception ex) {
			LOG.error("execute copy coprocessor action error.", ex);
		}
		
		return null;
	}
	
	@Override
	public String toString() {
		return "target:" + this.target;
	}
}
