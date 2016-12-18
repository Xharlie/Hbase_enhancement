/**
 * 
 */
package com.etao.hbase.coprocessor.copy.common;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * CopyCoprocessor related constants
 * @author lingbo.zm
 *
 */
public class CopyCoprocessorConstants {
	public static final String CONF_KEY_RULES = "rules";
    public static final String CONF_KEY_RULES_RELOAD_DELAY = "rules.reload.delay";
    public static final String CONF_KEY_RULES_RELOAD_PERIOD = "rules.reload.period";
    
    public static final long DEFAULT_RULES_RELOAD_DELAY = 180000;
    public static final long DEFAULT_RULES_RELOAD_PERIOD = 900000;
   
    public static final String JSON_WILDCHAR = "*"; 
    public static final String JSON_WILDCHAR_REGEX = "\\*";
	public static final String JSON_WILDCHAR_REPLACE = "[\\\\p{ASCII}]*";
	
    public static final String JSON_COLUMN = "column";
    
	public static final String JSON_CONDITIONS = "conditions";
    public static final String JSON_CONDITION_OPERATOR = "operator";
    public static final String JSON_CONDITION_OPERAND = "operand";
    
    public static final String JSON_ACTIONS = "actions";
    
    public static final String JSON_ACTION_TARGET = "target";
    
    public static final String JSON_PLACEHOLDER_COLUMN = "${column}";
	public static final String JSON_PLACEHOLDER_QUALIFIER = "${qualifier}";
	
    public static final byte[] JSON_PLACEHOLDER_COLUMN_BYTES = Bytes.toBytes(JSON_PLACEHOLDER_COLUMN);
	public static final byte[] JSON_PLACEHOLDER_QUALIFIER_BYTES = Bytes.toBytes(JSON_PLACEHOLDER_QUALIFIER);
}
