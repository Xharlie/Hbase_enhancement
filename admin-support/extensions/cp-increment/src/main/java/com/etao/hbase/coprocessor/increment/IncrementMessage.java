/**
 * 
 */
package com.etao.hbase.coprocessor.increment;

import com.etao.hadoop.hbase.queue.client.Message;

/**
 * @author yutian.xb
 *
 */
public class IncrementMessage {
    private String target;
    private Message message;

    public IncrementMessage(String target, Message message) {
	this.target = target;
	this.message = message;
    }

    /**
     * @return the target hqueue name it belongs to
     */
    public String getQueueName() {
	return this.target;
    }


    /**
     * @return the {@link Message} object to put to target hqueue
     */
    public Message getTargetMessage() {
	return this.message;
    }

}

