/**
 * 
 */
package com.etao.hbase.coprocessor.compare.rule;


/**
 * @author dihong.wq
 * 
 */
public class Trigger {
	private Operator operator;

	private String value;

	@Override
	public String toString() {
		return "[operator=" + operator + ", value=" + value + "]";
	}

	public Operator getOperator() {
		return operator;
	}

	public void setOperator(Operator operator) {
		this.operator = operator;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
}
