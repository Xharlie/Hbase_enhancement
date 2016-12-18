/**
 * 
 */
package com.etao.hbase.coprocessor.increment.config;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author yutian.xb
 *
 */
public class Condition {
    private static final Log LOG = LogFactory.getLog(Condition.class);

    public enum OperatorType {
	STRING_EQ, STRING_NE, INT_EQ, INT_NE, INT_LT, INT_LE, INT_GT, INT_GE, FLOAT_EQ, FLOAT_NE, FLOAT_LT, FLOAT_LE, FLOAT_GT, FLOAT_GE
    }

    private byte[][] source;
    private OperatorType operator;
    private String value;

    public String getSource() {
	if (null != source && 2 == source.length) {
	    return Bytes.toString(KeyValue.makeColumn(source[0], source[1]));
	}

	return null;
    }

    public String getOperator() {
	if (null != operator) {
	    return operator.toString();
	}

	return null;
    }

    public String getValue() {
	return this.value;
    }

    public Condition(JSONObject json) throws JSONException {
	if (null == json) {
	    return;
	}

	if (!json.has("source") || !json.has("operator") || !json.has("value")) {
	    return;
	}
	
	source = KeyValue.parseColumn(Bytes.toBytes(json.getString("source")));
	try {
	    operator = OperatorType.valueOf(json.getString("operator").trim());
	} catch (IllegalArgumentException e) {
	    LOG.warn(e.toString());
	}
	value = json.getString("value");
    }

    public boolean verify(Put put) {
	if (null == source || null == operator || null == value) {
	    return false;
	}

	if (null == put || !put.has(source[0], source[1])) {
	    return false;
	}

	List<Cell> cellList = put.get(source[0], source[1]);
	long ts = 0;
	String sourceValue = null;
	for (Cell cell : cellList) {
	    if (cell.getTimestamp() > ts) {
		ts = cell.getTimestamp();
		sourceValue = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
	    }
	}

	boolean pass = false;
	switch (operator) {
	case STRING_EQ:
	    if (sourceValue.equals(value)) {
		pass = true;
	    }
	    break;
	case STRING_NE:
	    if (!sourceValue.equals(value)) {
		pass = true;
	    }
	    break;
	case INT_EQ:
	    try {
		int sourceInt = Integer.parseInt(sourceValue);
		int valueInt = Integer.parseInt(value);
		if (sourceInt == valueInt) {
		    pass = true;
		}
	    } catch (NumberFormatException e) {
	    }
	    break;
	case INT_NE:
	    try {
		int sourceInt = Integer.parseInt(sourceValue);
		int valueInt = Integer.parseInt(value);
		if (sourceInt != valueInt) {
		    pass = true;
		}
	    } catch (NumberFormatException e) {
	    }
	    break;
	case INT_LT:
	    try {
		int sourceInt = Integer.parseInt(sourceValue);
		int valueInt = Integer.parseInt(value);
		if (sourceInt < valueInt) {
		    pass = true;
		}
	    } catch (NumberFormatException e) {
	    }
	    break;
	case INT_LE:
	    try {
		int sourceInt = Integer.parseInt(sourceValue);
		int valueInt = Integer.parseInt(value);
		if (sourceInt <= valueInt) {
		    pass = true;
		}
	    } catch (NumberFormatException e) {
	    }
	    break;
	case INT_GT:
	    try {
		int sourceInt = Integer.parseInt(sourceValue);
		int valueInt = Integer.parseInt(value);
		if (sourceInt > valueInt) {
		    pass = true;
		}
	    } catch (NumberFormatException e) {
	    }
	    break;
	case INT_GE:
	    try {
		int sourceInt = Integer.parseInt(sourceValue);
		int valueInt = Integer.parseInt(value);
		if (sourceInt >= valueInt) {
		    pass = true;
		}
	    } catch (NumberFormatException e) {
	    }
	    break;
	case FLOAT_EQ:
	    try {
		float sourceInt = Float.parseFloat(sourceValue);
		float valueInt = Float.parseFloat(value);
		if (sourceInt == valueInt) {
		    pass = true;
		}
	    } catch (NumberFormatException e) {
	    }
	    break;
	case FLOAT_NE:
	    try {
		float sourceInt = Float.parseFloat(sourceValue);
		float valueInt = Float.parseFloat(value);
		if (sourceInt != valueInt) {
		    pass = true;
		}
	    } catch (NumberFormatException e) {
	    }
	    break;
	case FLOAT_LT:
	    try {
		float sourceInt = Float.parseFloat(sourceValue);
		float valueInt = Float.parseFloat(value);
		if (sourceInt < valueInt) {
		    pass = true;
		}
	    } catch (NumberFormatException e) {
	    }
	    break;
	case FLOAT_LE:
	    try {
		float sourceInt = Float.parseFloat(sourceValue);
		float valueInt = Float.parseFloat(value);
		if (sourceInt <= valueInt) {
		    pass = true;
		}
	    } catch (NumberFormatException e) {
	    }
	    break;
	case FLOAT_GT:
	    try {
		float sourceInt = Float.parseFloat(sourceValue);
		float valueInt = Float.parseFloat(value);
		if (sourceInt > valueInt) {
		    pass = true;
		}
	    } catch (NumberFormatException e) {
	    }
	    break;
	case FLOAT_GE:
	    try {
		float sourceInt = Float.parseFloat(sourceValue);
		float valueInt = Float.parseFloat(value);
		if (sourceInt >= valueInt) {
		    pass = true;
		}
	    } catch (NumberFormatException e) {
	    }
	    break;
	default:
	    LOG.warn("Unknown condition operator: [" + operator.toString() + "]!");
	    break;
	}

	return pass;
    }
}

