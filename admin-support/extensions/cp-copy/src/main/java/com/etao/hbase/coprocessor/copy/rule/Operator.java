/**
 * 
 */
package com.etao.hbase.coprocessor.copy.rule;

/**
 * trigger operator for CopyCoprocessor rule
 * @author lingbo.zm
 *
 */
public enum Operator {
	/** numeric equal */
	NEQ,

	/** numeric not equal */
	NNE,
	
	/** numeric less than */
	NLT,
	
	/** numeric less or equal */
	NLE,
	
	/** numeric greater than */
	NGT,
	
	/** numeric greater or equal */
	NGE,
	
	/** alphabet equal */
	SEQ,
	
	/** alphabet not equal */
	SNE
}
