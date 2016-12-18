/**
 * 
 */
package com.etao.hbase.coprocessor.copy.rule;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.json.JSONArray;
import org.json.JSONObject;

import com.etao.hbase.coprocessor.copy.common.CopyCoprocessorConstants;

/**
 * rule for CopyCoprocessor
 * @author lingbo.zm
 *
 */
public class CopyRule {
	private static final Log LOG = LogFactory.getLog(CopyRule.class);
	
	private Column column;
	private List<Condition> conditionList = new LinkedList<Condition>();
	private List<Action> actionList = new LinkedList<Action>();
	
	public CopyRule(JSONObject json) throws Exception {
	    column = new Column(json.getString(CopyCoprocessorConstants.JSON_COLUMN));
		
		if (json.has(CopyCoprocessorConstants.JSON_CONDITIONS)) {
			JSONArray conditionArr = json.getJSONArray(CopyCoprocessorConstants.JSON_CONDITIONS);
			for (int i=0; i<conditionArr.length(); i++) {
				Condition condition = new Condition(conditionArr.getJSONObject(i));
				conditionList.add(condition);
			}
		}
		
		JSONArray actionArr = json.getJSONArray(CopyCoprocessorConstants.JSON_ACTIONS);
	    for (int i=0; i<actionArr.length(); i++) {
	        Action action = new Action(actionArr.getJSONObject(i));
	        actionList.add(action);
	    }
	}
	
	public List<Cell> apply(Put put) {
		List<Cell> totalKVPairsForRule = new LinkedList<Cell>();

		Map<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
	    
		for (Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
	    	List<Cell> kvs = entry.getValue();
	    	
	    	for (Cell kv : kvs) {
	    		try {
	    			if (!column.matches(kv)) {
		    			continue;
	    			}
		    		
		    		if (!verifyConditions(put, kv)) {
		    			continue;
		    		}
		    	
		    		List<Cell> kvsAddedForActions = executeActions(kv);
		    		if (kvsAddedForActions != null) {
		    			totalKVPairsForRule.addAll(kvsAddedForActions);
		    		}
	    		} catch (Exception ex) {
	    			LOG.error("apply copy coprocessor rule error.", ex);
	    		}
	    	}
	    }
	    
	    return totalKVPairsForRule;
	}
	
	private boolean verifyConditions(Put put, Cell kv) {
		for (Condition condition : conditionList) {
			boolean pass = condition.verify(put, kv);
			if (!pass) {
				return false;
			}
		}
		
		return true;
	}
	
	private List<Cell> executeActions(Cell kv) {
		List<Cell> kvs = new LinkedList<Cell>();
		
    	for (Action action : actionList) {
    		Cell kvAddedForAction = action.execute(kv);
    	    if (kvAddedForAction != null) {
    	    	kvs.add(kvAddedForAction);
    	    }
    	}
    	
    	return kvs;
    }
	
	@Override
	public String toString() {
		return "{column:" + this.column + ", conditions:" + this.conditionList + ", actions:" + this.actionList + "}";
	}
}
