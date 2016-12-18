/**
 * 
 */
package com.etao.hbase.coprocessor.compare.rule;

/**
 * @author dihong.wq
 * 
 */
public enum Operator {
	/**
	 * numeric equal
	 */
	NEQ,

	/**
	 * numeric not equal
	 */
	NNE,
	/**
	 * numeric less than
	 */
	NLT,
	/**
	 * numeric less or equal
	 */
	NLE,
	/**
	 * numeric greater than
	 */
	NGT,
	/**
	 * numeric greater or equal
	 */
	NGE,
	/**
	 * alphabet equal
	 */
	SEQ,
	/**
	 * alphabet not equal
	 */
	SNE
}
