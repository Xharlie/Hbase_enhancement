/**
 * 
 */
package com.etao.hbase.coprocessor.trace.rule;

import java.util.regex.Pattern;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import com.etao.hbase.coprocessor.trace.common.TraceCoprocessorConstants;

/**
 * column for TraceCoprocessor rule
 * @author lingbo.zm
 *
 */
public class Column {
	private String column;
	private String family;
	private String qualifier;
	private Pattern qualifierPattern = null;
	
    public Column(String column) {
    	this.column = column;
    	
    	int deliIdx = column.indexOf(KeyValue.COLUMN_FAMILY_DELIMITER);
		if (deliIdx > 0) {
		    family = column.substring(0, deliIdx);
		    qualifier = column.substring(deliIdx + 1);
		    
		    int qualifierRegIdx = qualifier.indexOf(TraceCoprocessorConstants.JSON_WILDCHAR);
			if (qualifierRegIdx >= 0) {
				qualifierPattern = Pattern.compile(qualifier.replaceAll(TraceCoprocessorConstants.JSON_WILDCHAR_REGEX, TraceCoprocessorConstants.JSON_WILDCHAR_REPLACE));
			}
		} else {
			family = column;
		}
    }
	
    public boolean matches(Cell kv) {
		if (!matchesFamily(Bytes.toString(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength()))) {
			return false;
		}
		
		if (!matchesQualifier(Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()))) {
			return false;
		}
		
		return true;
	}
	
	private boolean matchesFamily(String family) {
		return this.family.equals(family);
	}
	
	private boolean matchesQualifier(String qualifier) {
		if (this.qualifier == null || this.qualifier.isEmpty()) {
			// column qualifier not specified for trace rule
			return true;
		}
		
		return this.qualifierPattern == null ? this.qualifier.equals(qualifier) : this.qualifierPattern.matcher(qualifier).matches();	
	}

	@Override
	public String toString() {
		return this.column;
	}
}
