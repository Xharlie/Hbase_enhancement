/**
 * 
 */
package com.etao.hbase.coprocessor.increment.config;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.etao.hbase.coprocessor.increment.IncrementCoprocessor;
import com.etao.hbase.coprocessor.increment.IncrementMessage;
import com.etao.hbase.coprocessor.util.FileUtils;

/**
 * Rules for {@link IncrementCoprocessor}.
 * 
 * @author yutian.xb
 * 
 */
public class IncrementRules {
    private static final Log LOG = LogFactory.getLog(IncrementRules.class);
    private static final int DEFAULT_QUEUE_SIZE = 128;
    private static final int DEFAULT_WRITER_COUNT = 1;
    private static final int DEFAULT_WRITER_BATCH_SIZE = 128;
    private static final int DEFAULT_CLIENT_RPC_MAX_ATTEMPTS = 3;
    private static final int DEFAULT_CLIENT_RETRIES_NUM = 3;
    private static final long DEFAULT_RPC_TIMEOUT = 10000L;
    private static final long DEFAULT_QUEUE_OFFER_TIMEOUT = 3000L;
    private static final long DEFAULT_WRITER_WAIT_INTERVAL = 100L;
    private static final long DEFAULT_WRITER_EMPTY_INTERVAL = 1000L;
    private static final double DEFAULT_RETRY_MAX_RATIO = 0.7;
    private static final int DEFAULT_REQUEUE_MAX_NUM = 3;

    private boolean needReload;
    private long queueOfferTimeout = -1;

    /**
     * Reload with reconstructing blocking queue and async writers.
     */
    private Map<String, Integer> queueSizeMap = new TreeMap<String, Integer>();
    private Map<String, Integer> writerCountMap = new TreeMap<String, Integer>();
    private Map<String, Integer> clientRpcMaxAttemptsMap = new TreeMap<String, Integer>();
    private Map<String, Integer> clientRetriesNumMap = new TreeMap<String, Integer>();
    private Map<String, Long> rpcTimeoutMap = new TreeMap<String, Long>();

    /**
     * Reload without reconstructing blocking queue and async writers.
     */
    private Map<String, Integer> writerBatchSizeMap = new TreeMap<String, Integer>();
    private Map<String, Integer> requeueMaxNumMap = new TreeMap<String, Integer>();
    private Map<String, Double> retryMaxRatioMap = new TreeMap<String, Double>();
    private Map<String, Long> writerWaitIntervalMap = new TreeMap<String, Long>();
    private Map<String, Long> writerEmptyIntervalMap = new TreeMap<String, Long>();

    private List<Rule> ruleList = new LinkedList<Rule>();

    /**
     * Reload the increment coprocessor rules object from json string.
     */
    public static IncrementRules reload(String jsonStr) {
	if (null == jsonStr) {
	    return new IncrementRules();
	}

	try {
	    JSONObject json = new JSONObject(jsonStr);
	    return new IncrementRules(json);
	} catch (JSONException e) {
	    LOG.warn(e.toString());
	}

	return new IncrementRules();
    }

    /**
     * Create an empty instance, waiting for reload.
     */
    private IncrementRules() {
	needReload = true;
    }

    /**
     * Construct increment coprocessor rules object.
     */
    private IncrementRules(JSONObject json) throws JSONException {
	needReload = false;

	if (json.has("queue.offer.timeout")) {
	    queueOfferTimeout = json.getLong("queue.offer.timeout");
	}

	if (json.has("queues")) {
	    JSONObject queuesJson = json.getJSONObject("queues");
	    Iterator iter = queuesJson.keys();
	    while (iter.hasNext()) {
		String queueName = (String) iter.next();
		JSONObject queueJson = queuesJson.getJSONObject(queueName);

		queueSizeMap.put(queueName, DEFAULT_QUEUE_SIZE);
		if (queueJson.has("queue.size")) {
		    queueSizeMap.put(queueName, queueJson.getInt("queue.size"));
		}

		if (queueJson.has("writer.count")) {
		    writerCountMap.put(queueName, queueJson.getInt("writer.count"));
		}

		if (queueJson.has("writer.batch.size")) {
		    writerBatchSizeMap.put(queueName, queueJson.getInt("writer.batch.size"));
		}

		if (queueJson.has("client.rpc.max.attempts")) {
		    clientRpcMaxAttemptsMap.put(queueName, queueJson.getInt("client.rpc.max.attempts"));
		}

		if (queueJson.has("client.retries.num")) {
		    clientRetriesNumMap.put(queueName, queueJson.getInt("client.retries.num"));
		}

		if (queueJson.has("rpc.timeout")) {
		    rpcTimeoutMap.put(queueName, queueJson.getLong("rpc.timeout"));
		}

		if (queueJson.has("requeue.max.num")) {
		    requeueMaxNumMap.put(queueName, queueJson.getInt("requeue.max.num"));
		}

		if (queueJson.has("retry.max.ratio")) {
		    retryMaxRatioMap.put(queueName, queueJson.getDouble("retry.max.ratio"));
		}

		if (queueJson.has("writer.wait.interval")) {
		    writerWaitIntervalMap.put(queueName, queueJson.getLong("writer.wait.interval"));
		}

		if (queueJson.has("writer.empty.interval")) {
		    writerEmptyIntervalMap.put(queueName, queueJson.getLong("writer.empty.interval"));
		}
	    }
	}

	if (json.has("rules")) {
	    JSONArray rulesArr = json.getJSONArray("rules");
	    for (int i = 0; i < rulesArr.length(); ++i) {
		ruleList.add(new Rule(rulesArr.getJSONObject(i)));
	    }
	}
    }

    /**
     * @return whether the rules object needs reloading or not
     */
    public boolean needReload() {
	return this.needReload;
    }

    /**
     * Apply the rules on a single {@link Put} object.
     * 
     * @param put
     *            target {@link Put} object
     * @return the list of {@link IncrementMessage}s to push to blocking queues
     */
    public List<IncrementMessage> apply(Put put) {
	if (null == put) {
	    return null;
	}

	List<IncrementMessage> messages = null;
	for (Rule rule : ruleList) {
	    List<IncrementMessage> list = rule.apply(put);
	    if (null == list) {
		continue;
	    }

	    if (null == messages) {
		messages = new LinkedList<IncrementMessage>();
	    }

	    messages.addAll(list);
	}

	return messages;
    }

    /**
     * @return a map for blocking queues' names and corresponding sizes
     */
    public Map<String, Integer> getQueueSizeMap() {
	return queueSizeMap;
    }

    /**
     * @return the timeout for pushing {@link IncrementMessage}s into blocking
     *         queues
     */
    public long getOfferTimeout() {
	if (0 <= queueOfferTimeout) {
	    return queueOfferTimeout;
	}

	return DEFAULT_QUEUE_OFFER_TIMEOUT;
    }

    /**
     * @return the num of async writer threads for blocking queue
     */
    public int getWriterCount(String queueName) {
	if (null == queueName || !writerCountMap.containsKey(queueName)) {
	    return DEFAULT_WRITER_COUNT;
	}

	return writerCountMap.get(queueName);
    }

    /**
     * @return the batch size of each {@code drainTo} for blocking queue
     */
    public int getWriterBatchSize(String queueName) {
	if (null == queueName || !writerBatchSizeMap.containsKey(queueName)) {
	    return DEFAULT_WRITER_BATCH_SIZE;
	}

	return writerBatchSizeMap.get(queueName);
    }

    /**
     * @return client rpc max attempts for target hqueue
     */
    public int getClientRpcMaxAttempts(String queueName) {
	if (null == queueName || !clientRpcMaxAttemptsMap.containsKey(queueName)) {
	    return DEFAULT_CLIENT_RPC_MAX_ATTEMPTS;
	}

	return clientRpcMaxAttemptsMap.get(queueName);
    }

    /**
     * @return client retries num for target hqueue
     */
    public int getClientRetriesNum(String queueName) {
	if (null == queueName || !clientRetriesNumMap.containsKey(queueName)) {
	    return DEFAULT_CLIENT_RETRIES_NUM;
	}

	return clientRetriesNumMap.get(queueName);
    }

    /**
     * @return rpc timeout for target hqueue
     */
    public long getRpcTimeout(String queueName) {
	if (null == queueName || !rpcTimeoutMap.containsKey(queueName)) {
	    return DEFAULT_RPC_TIMEOUT;
	}

	return rpcTimeoutMap.get(queueName);
    }

    public int getRequeueMaxNum(String queueName) {
	if (null == queueName || !requeueMaxNumMap.containsKey(queueName)) {
	    return DEFAULT_REQUEUE_MAX_NUM;
	}

	return requeueMaxNumMap.get(queueName);
    }

    public double getRetryMaxRatio(String queueName) {
	if (null == queueName || !retryMaxRatioMap.containsKey(queueName)) {
	    return DEFAULT_RETRY_MAX_RATIO;
	}

	return retryMaxRatioMap.get(queueName);
    }

    public long getWriterWaitInterval(String queueName) {
	if (null == queueName || !writerWaitIntervalMap.containsKey(queueName)) {
	    return DEFAULT_WRITER_WAIT_INTERVAL;
	}

	return writerWaitIntervalMap.get(queueName);
    }

    public long getWriterEmptyInterval(String queueName) {
	if (null == queueName || !writerEmptyIntervalMap.containsKey(queueName)) {
	    return DEFAULT_WRITER_EMPTY_INTERVAL;
	}

	return writerEmptyIntervalMap.get(queueName);
    }

    /**
     * @return rule list
     */
    public List<Rule> getRules() {
	return ruleList;
    }

    private IncrementRulesDiff diff(IncrementRules oldRules) {
	IncrementRulesDiff rulesDiff = null;
	List<String> addList = null;
	List<String> changeList = null;
	List<String> dropList = null;
	Map<String, Integer> oldQueueSizeMap = oldRules.getQueueSizeMap();
	for (Entry<String, Integer> entry : queueSizeMap.entrySet()) {
	    String queueName = entry.getKey();
	    if (!oldQueueSizeMap.containsKey(queueName)) {
		if (null == addList) {
		    addList = new LinkedList<String>();
		}

		addList.add(queueName);
		continue;
	    }

	    int queueSize = entry.getValue();
	    int oldQueueSize = oldQueueSizeMap.get(queueName);
	    if (queueSize != oldQueueSize) {
		if (null == changeList) {
		    changeList = new LinkedList<String>();
		}

		changeList.add(queueName);
		continue;
	    }

	    int newWriterCount = this.getWriterCount(queueName);
	    int oldWriterCount = oldRules.getWriterCount(queueName);
	    if (newWriterCount != oldWriterCount) {
		if (null == changeList) {
		    changeList = new LinkedList<String>();
		}

		changeList.add(queueName);
		continue;
	    }

	    int newClientRpcMaxAttempts = this.getClientRpcMaxAttempts(queueName);
	    int oldClientRpcMaxAttempts = oldRules.getClientRpcMaxAttempts(queueName);
	    if (newClientRpcMaxAttempts != oldClientRpcMaxAttempts) {
		if (null == changeList) {
		    changeList = new LinkedList<String>();
		}

		changeList.add(queueName);
		continue;
	    }

	    int newClientRetriesNum = this.getClientRetriesNum(queueName);
	    int oldClientRetriesNum = oldRules.getClientRetriesNum(queueName);
	    if (newClientRetriesNum != oldClientRetriesNum) {
		if (null == changeList) {
		    changeList = new LinkedList<String>();
		}

		changeList.add(queueName);
		continue;
	    }

	    long newRpcTimeout = this.getRpcTimeout(queueName);
	    long oldRpcTimeout = oldRules.getRpcTimeout(queueName);
	    if (newRpcTimeout != oldRpcTimeout) {
		if (null == changeList) {
		    changeList = new LinkedList<String>();
		}

		changeList.add(queueName);
		continue;
	    }
	}

	for (Entry<String, Integer> entry : oldQueueSizeMap.entrySet()) {
	    String queueName = entry.getKey();
	    if (!queueSizeMap.containsKey(queueName)) {
		if (null == dropList) {
		    dropList = new LinkedList<String>();
		}

		dropList.add(queueName);
	    }
	}

	if (null == addList && null == changeList && null == dropList) {
	    return null;
	}

	rulesDiff = new IncrementRulesDiff();
	if (null != addList) {
	    rulesDiff.setAddList(addList.toArray(new String[0]));
	}

	if (null != changeList) {
	    rulesDiff.setChangeList(changeList.toArray(new String[0]));
	}

	if (null != dropList) {
	    rulesDiff.setDropList(dropList.toArray(new String[0]));
	}

	return rulesDiff;
    }

    /**
     * Differ two {@link IncrementRules}s.
     * 
     * @return
     */
    public static IncrementRulesDiff diff(IncrementRules newRules, IncrementRules oldRules) {
	if (null == newRules || null == oldRules) {
	    return null;
	}

	return newRules.diff(oldRules);
    }

    /**
     * A tool to verify the rules config file.
     */
    public static void main(String[] args) {
	if (0 >= args.length || 3 <= args.length) {
	    System.err.println("Usage: exec <rule1_path> [rule2_path]");
	    System.err.println("    - use SINGLE rule_path to verify the rules config file;");
	    System.err.println("    - use DOUBLE rule_paths to differ two rules config files;");
	    System.exit(1);
	}

	Configuration conf = HBaseConfiguration.create();
	String rulePath1 = args[0];
	String jsonStr1 = null;
	if (rulePath1.startsWith("hdfs://")) {
	    jsonStr1 = FileUtils.readContentFromHdfs(conf, rulePath1);
	} else {
	    jsonStr1 = FileUtils.readContentFromLocal(rulePath1);
	}

	if (1 == args.length) {
	    // verify single rules config file
	    try {
		new IncrementRules(new JSONObject(jsonStr1));
		System.out.println("Verify success!");
	    } catch (Exception e) {
		System.out.println("Verify fail!");
		e.printStackTrace();
	    }

	    System.exit(0);
	} else {
	    // differ double rules config files
	    String rulePath2 = args[1];
	    String jsonStr2 = null;
	    if (rulePath2.startsWith("hdfs://")) {
		jsonStr2 = FileUtils.readContentFromHdfs(conf, rulePath2);
	    } else {
		jsonStr2 = FileUtils.readContentFromLocal(rulePath2);
	    }

	    try {
		IncrementRules rules1 = new IncrementRules(new JSONObject(jsonStr1));
		IncrementRules rules2 = new IncrementRules(new JSONObject(jsonStr2));
		IncrementRulesDiff rulesDiff = IncrementRules.diff(rules1, rules2);
		if (null == rulesDiff) {
		    System.out.println("Same rules!");
		} else {
		    System.out.println("Different rules!");
		    System.out.println(rulesDiff.toJson().toString());
		}
	    } catch (JSONException e) {
		e.printStackTrace();
	    }

	    System.exit(0);
	}
    }
}
