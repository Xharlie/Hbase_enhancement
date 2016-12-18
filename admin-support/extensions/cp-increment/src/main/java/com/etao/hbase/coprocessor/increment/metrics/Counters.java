/**
 * 
 */
package com.etao.hbase.coprocessor.increment.metrics;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * @author yutian.xb
 *
 */
public class Counters {
    private Map<String, Long> pushItemCounters;
    private Map<String, Long> pushBatchCounters;
    private Map<String, Long> dropItemCounters;
    private Map<String, Long> dropBatchCounters;

    public Counters() {
	pushItemCounters = new TreeMap<String, Long>();
	pushBatchCounters = new TreeMap<String, Long>();
	dropItemCounters = new TreeMap<String, Long>();
	dropBatchCounters = new TreeMap<String, Long>();
    }

    public void increasePush(String queueName, long delta) {
	if (0 != delta) {
	    long newVal = delta;
	    if (pushItemCounters.containsKey(queueName)) {
		newVal += pushItemCounters.get(queueName);
	    }
	    pushItemCounters.put(queueName, newVal);

	    newVal = 1;
	    if (pushBatchCounters.containsKey(queueName)) {
		newVal += pushBatchCounters.get(queueName);
	    }
	    pushBatchCounters.put(queueName, newVal);
	}
    }

    public void increaseDrop(String queueName, long delta) {
	if (0 != delta) {
	    long newVal = delta;
	    if (dropItemCounters.containsKey(queueName)) {
		newVal += dropItemCounters.get(queueName);
	    }
	    dropItemCounters.put(queueName, newVal);

	    newVal = 1;
	    if (dropBatchCounters.containsKey(queueName)) {
		newVal += dropBatchCounters.get(queueName);
	    }
	    dropBatchCounters.put(queueName, newVal);
	}
    }

    public long getPushItemCount(String queueName) {
	if (pushItemCounters.containsKey(queueName)) {
	    return pushItemCounters.get(queueName);
	}

	return 0;
    }

    public long getPushBatchCount(String queueName) {
	if (pushBatchCounters.containsKey(queueName)) {
	    return pushBatchCounters.get(queueName);
	}

	return 0;
    }

    public long getDropItemCount(String queueName) {
	if (dropItemCounters.containsKey(queueName)) {
	    return dropItemCounters.get(queueName);
	}

	return 0;
    }

    public long getDropBatchCount(String queueName) {
	if (dropBatchCounters.containsKey(queueName)) {
	    return dropBatchCounters.get(queueName);
	}

	return 0;
    }

    public long getTotalPushItemCount() {
	long count = 0;
	for (Entry<String, Long> entry : pushItemCounters.entrySet()) {
	    count += entry.getValue();
	}

	return count;
    }

    public long getTotalPushBatchCount() {
	long count = 0;
	for (Entry<String, Long> entry : pushBatchCounters.entrySet()) {
	    count += entry.getValue();
	}

	return count;
    }

    public long getTotalDropItemCount() {
	long count = 0;
	for (Entry<String, Long> entry : dropItemCounters.entrySet()) {
	    count += entry.getValue();
	}

	return count;
    }

    public long getTotalDropBatchCount() {
	long count = 0;
	for (Entry<String, Long> entry : dropBatchCounters.entrySet()) {
	    count += entry.getValue();
	}

	return count;
    }

    public static void main(String[] args) {
    }
}

