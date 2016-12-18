/**
 * 
 */
package com.etao.hbase.coprocessor.trace.common;

/**
 * TraceCoprocessor related constants
 * @author lingbo.zm
 *
 */
public class TraceCoprocessorConstants {
	public static final String CONF_KEY_RULES = "rules";
    public static final String CONF_KEY_RULES_RELOAD_DELAY = "rules.reload.delay";
    public static final String CONF_KEY_RULES_RELOAD_PERIOD = "rules.reload.period";
    
    public static final long DEFAULT_RULES_RELOAD_DELAY = 180000;
    public static final long DEFAULT_RULES_RELOAD_PERIOD = 900000;
   
    public static final String JSON_WILDCHAR = "*"; 
    public static final String JSON_WILDCHAR_REGEX = "\\*";
	public static final String JSON_WILDCHAR_REPLACE = "[\\\\p{ASCII}]*";
}
