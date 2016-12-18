/**
 * 
 */
package com.etao.hbase.coprocessor.copy.rule;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;

import com.etao.hbase.coprocessor.copy.common.CopyCoprocessorConstants;

/**
 * condition for copy rule
 * @author lingbo.zm
 *
 */
public class Condition {
    private static final Log LOG = LogFactory.getLog(Condition.class);
    
    private Operator operator;
    private String operand;
    
    public Condition(JSONObject json) throws Exception {
    	operator = Operator.valueOf(json.getString(CopyCoprocessorConstants.JSON_CONDITION_OPERATOR).trim());
	
    	operand = json.getString(CopyCoprocessorConstants.JSON_CONDITION_OPERAND);
    }
    
    public boolean verify(Put put, Cell kv) {
    	if (null == operator || null == operand) {
    		return false;
    	}
    	
    	boolean pass = false;
    
    	try {
    		ValuePair pair = processSourceAndDestinationValuePair(put, kv);
    		
        	String sourceValue = pair.getSourceValue();
        	String destValue = pair.getDestValue();
    		
    		switch (this.operator) {
    		case NEQ:
    			if (sourceValue != null && destValue != null) {
    				try {
    					float sourceVal = Float.parseFloat(sourceValue);
    					float destVal = Float.parseFloat(destValue);

    					pass = sourceVal == destVal;
    				} catch (NumberFormatException e) {
    				}
    			}
    			break;
    		case NNE:
    			if (sourceValue == null || destValue == null) {
    				pass = true;
    			} else {
    				try {
    					float sourceVal = Float.parseFloat(sourceValue);
    					float destVal = Float.parseFloat(destValue);

    					pass = sourceVal != destVal;
    				} catch (NumberFormatException e) {
    				}
    			}
    			break;
    		case NLT:
    			if (sourceValue != null && destValue != null) {
    				try {
    					float sourceVal = Float.parseFloat(sourceValue);
    					float destVal = Float.parseFloat(destValue);

    					pass = sourceVal < destVal;
    				} catch (NumberFormatException e) {
    				}
    			}
    			break;
    		case NLE:
    			if (sourceValue != null && destValue != null) {
    				try {
    					float sourceVal = Float.parseFloat(sourceValue);
    					float destVal = Float.parseFloat(destValue);

    					pass = sourceVal <= destVal;
    				} catch (NumberFormatException e) {
    				}
    			}
    			break;
    		case NGT:
    			if (sourceValue != null && destValue != null) {
    				try {
    					float sourceVal = Float.parseFloat(sourceValue);
    					float destVal = Float.parseFloat(destValue);

    					pass = sourceVal > destVal;
    				} catch (NumberFormatException e) {
    				}
    			}
    			break;
    		case NGE:
    			if (sourceValue != null && destValue != null) {
    				try {
    					float sourceVal = Float.parseFloat(sourceValue);
    					float destVal = Float.parseFloat(destValue);

    					pass = sourceVal >= destVal;
    				} catch (NumberFormatException e) {
    				}
    			}
    			break;
    		case SEQ:
    			if (sourceValue != null && destValue != null) {
    				pass = sourceValue.equals(destValue);
    			}
    			break;
    		case SNE:
    			if (sourceValue == null || destValue == null) {
    				pass = true;
    			} else {
    				pass = !sourceValue.equals(destValue);
    			}
    			break;
    		default:
    			break;
    		}	
    	} catch (Exception ex) {
    	    LOG.error("verify condition error.", ex);	
    	}

		return pass;
    }
   
    private ValuePair processSourceAndDestinationValuePair(Put put, Cell kv) throws Exception {
    	ValuePair pair = new ValuePair();
    	
    	pair.setSourceValue(Bytes.toString(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()));
    	
    	int deliIdx = operand.indexOf(KeyValue.COLUMN_FAMILY_DELIMITER);
    	if (deliIdx > 0) {
    		byte[] destFamily = Bytes.toBytes(operand.substring(0, deliIdx));
    		byte[] destQualifier = Bytes.toBytes(operand.substring(deliIdx + 1));
    		
    		String destValue = fetchColumnLatestInPut(put, destFamily, destQualifier);
    		
    		pair.setDestValue(destValue);
    	} else {
    		pair.setDestValue(operand);
    	}	
    	
    	return pair;
    }
    
    private String fetchColumnLatestInPut(Put put, byte[] family, byte[] qualifier) {
    	long ts = 0L;
    	String value = null;
	
    	List<Cell> kvList = put.get(family, qualifier);
    	for (Cell kv : kvList) {
    	    if (kv.getTimestamp() > ts) {
    	    	ts = kv.getTimestamp();
    		    value = Bytes.toString(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
    	    }
    	}
    	
    	return value;
    }
    
    private class ValuePair {
    	String sourceValue;
    	
    	String destValue;

		public String getSourceValue() {
			return sourceValue;
		}

		public void setSourceValue(String sourceValue) {
			this.sourceValue = sourceValue;
		}

		public String getDestValue() {
			return destValue;
		}

		public void setDestValue(String destValue) {
			this.destValue = destValue;
		}
    }
    
    @Override
    public String toString() {
    	return "operator:" + this.operator + ", operand:" + this.operand;
    }
}
