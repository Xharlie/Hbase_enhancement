/**
 * 
 */
package com.etao.hbase.coprocessor.compare.rule;

/**
 * 
 * @author lingbo.zm
 *
 */
public class Action {
	private String column;
	private String value;

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	@Override
	public String toString() {
		return "[column=" + column + ", value=" + value + "]";
	}
}
