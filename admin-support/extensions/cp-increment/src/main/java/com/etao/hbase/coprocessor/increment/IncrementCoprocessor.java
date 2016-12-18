/**
 * 
 */
package com.etao.hbase.coprocessor.increment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.etao.hadoop.hbase.queue.client.HQueue;
import com.etao.hadoop.hbase.queue.client.Message;
import com.etao.hbase.coprocessor.increment.config.IncrementRules;
import com.etao.hbase.coprocessor.increment.config.IncrementRulesDiff;
import com.etao.hbase.coprocessor.increment.metrics.Counters;
import com.etao.hbase.coprocessor.util.CoprocessorUtils;
import com.etao.hbase.coprocessor.util.FileUtils;

/**
 * @author yutian.xb
 */
public class IncrementCoprocessor extends BaseRegionObserver {
    private static final Log LOG = LogFactory.getLog(IncrementCoprocessor.class);
    private static final String COPROCESSOR_NAME = "Increment Coprocessor";
    private static final String CONF_KEY_RULES_DIR = "rules.dir";
    private static final String CONF_KEY_RULES_RELOAD_DELAY = "rules.reload.delay";
    private static final String CONF_KEY_RULES_RELOAD_PERIOD = "rules.reload.period";
    private static final String CONF_KEY_MONITOR_DELAY = "monitor.delay";
    private static final String CONF_KEY_MONITOR_PERIOD = "monitor.period";
    private static final long DEFAULT_RULES_RELOAD_DELAY = 60000;
    private static final long DEFAULT_RULES_RELOAD_PERIOD = 600000;
    private static final long DEFAULT_MONITOR_DELAY = 15000;
    private static final long DEFAULT_MONITOR_PERIOD = 150000;

    private static ReentrantLock REFERENCE_COUNTER_LOCK = new ReentrantLock();
    private static ReadWriteLock RULES_LOCK = new ReentrantReadWriteLock();
    private static ReadWriteLock QUEUE_LOCK = new ReentrantReadWriteLock();
    private static ReadWriteLock COUNTERS_LOCK = new ReentrantReadWriteLock();

    private String tableName;
    private String regionName;
    private String rules_key;
    private String reload_timer_key;
    private String monitor_timer_key;
    private String reference_counter_key;
    private String counters_key;
    private String queue_key;
    private String writer_key;

    class MonitorTimerTask extends TimerTask {
	private Configuration conf;
	private RegionCoprocessorEnvironment rce;
	private Map<String, Object> sharedData;

	public MonitorTimerTask(Configuration conf, RegionCoprocessorEnvironment rce) {
	    this.conf = conf;
	    this.rce = rce;
	}

	@Override
	public void run() {
	    sharedData = rce.getSharedData();

	    Counters counters = null;
	    COUNTERS_LOCK.readLock().lock();
	    try {
		counters = (Counters) sharedData.get(counters_key);
		if (null == counters) {
		    counters = new Counters();
		}
	    } finally {
		COUNTERS_LOCK.readLock().unlock();
	    }

	    QUEUE_LOCK.readLock().lock();
	    try {
		Map<String, BlockingQueue<IncrementMessage>> queueMap = (Map<String, BlockingQueue<IncrementMessage>>) sharedData.get(queue_key);
		Map<String, Map<String, AsyncWriterThread>> queueWritersMap = (Map<String, Map<String, AsyncWriterThread>>) sharedData.get(writer_key);
		if (null != queueMap && null != queueWritersMap) {
		    for (Entry<String, BlockingQueue<IncrementMessage>> entry : queueMap.entrySet()) {
			String queueName = entry.getKey();
			BlockingQueue<IncrementMessage> queue = entry.getValue();
			int queueSize = queue.size();
			int queueCapacity = queue.remainingCapacity();
			int queueWriters = 0;
			if (queueWritersMap.containsKey(queueName)) {
			    queueWriters = queueWritersMap.get(queueName).size();
			}

			long pushItemCount = counters.getPushItemCount(queueName);
			long pushBatchCount = counters.getPushBatchCount(queueName);
			long dropItemCount = counters.getDropItemCount(queueName);
			long dropBatchCount = counters.getDropBatchCount(queueName);

			LOG.info("[" + tableName + "=>" + queueName + "] writers: " + queueWriters + ", queue: " + queueSize + "/" + (queueSize + queueCapacity) + ", push: "
				+ pushItemCount + "/" + pushBatchCount + ", drop: " + dropItemCount + "/" + dropBatchCount);
		    }
		}
	    } finally {
		QUEUE_LOCK.readLock().unlock();
	    }

	    long totalPushItemCount = counters.getTotalPushItemCount();
	    long totalPushBatchCount = counters.getTotalPushBatchCount();
	    long totalDropItemCount = counters.getTotalDropItemCount();
	    long totalDropBatchCount = counters.getTotalDropBatchCount();
	    LOG.info("[" + tableName + "] total push: " + totalPushItemCount + "/" + totalPushBatchCount + ", total drop: " + totalDropItemCount + "/" + totalDropBatchCount);
	}
    }

    class RulesReloadTimerTask extends TimerTask {
	private Configuration conf;
	private RegionCoprocessorEnvironment rce;
	private Map<String, Object> sharedData;

	public RulesReloadTimerTask(Configuration conf, RegionCoprocessorEnvironment rce) {
	    this.conf = conf;
	    this.rce = rce;
	}

	@Override
	public void run() {
	    sharedData = rce.getSharedData();

	    REFERENCE_COUNTER_LOCK.lock();
	    try {
		LOG.info("Start reloading configs for [" + tableName + "]...");

		AtomicInteger ai = (AtomicInteger) sharedData.get(reference_counter_key);
		if (null != ai) {
		    LOG.info("Reference count for [" + tableName + "] is: " + ai.get());
		}

		String rulesPath = conf.get(CONF_KEY_RULES_DIR) + "/" + tableName + ".json";
		String jsonStr = FileUtils.readContentFromHdfs(conf, rulesPath);
		IncrementRules newRules = IncrementRules.reload(jsonStr);
		if (newRules.needReload()) {
		    if (null == jsonStr || jsonStr.trim().isEmpty()) {
			LOG.warn("The rules file [" + rulesPath + "] doesn't exist or it's empty, skip it!");
		    } else {
			LOG.warn("The rules file [" + rulesPath + "] is illegal, skip it!");
		    }
		} else {
		    LOG.info("Reload coprocessor rules for [" + tableName + "]");
		}

		IncrementRules oldRules = null;
		RULES_LOCK.writeLock().lock();
		try {
		    oldRules = (IncrementRules) sharedData.get(rules_key);
		    if (null == oldRules) {
			LOG.warn("The old rules for [" + tableName + "] had already been released, skip it!");
			return;
		    }

		    sharedData.put(rules_key, newRules);
		} finally {
		    RULES_LOCK.writeLock().unlock();
		}

		if (!newRules.needReload()) {
		    IncrementRulesDiff rulesDiff = IncrementRules.diff(newRules, oldRules);
		    if (null == rulesDiff) {
			LOG.info("There's no difference between two rules for [" + tableName + "], skip it.");
		    } else {
			String[] addList = rulesDiff.getAddList();
			String[] changeList = rulesDiff.getChangeList();
			String[] dropList = rulesDiff.getDropList();

			QUEUE_LOCK.writeLock().lock();
			try {
			    if (null == addList || 0 == addList.length) {
				LOG.info("Add queue for [" + tableName + "]: 0");
			    } else {
				LOG.info("Add queue for [" + tableName + "]: " + addList.length);
				addQueue(addList, newRules);
			    }

			    if (null == changeList || 0 == changeList.length) {
				LOG.info("Change queue for [" + tableName + "]: 0");
			    } else {
				LOG.info("Change queue for [" + tableName + "]: " + changeList.length);
				changeQueue(changeList, newRules, oldRules);
			    }

			    if (null == dropList || 0 == dropList.length) {
				LOG.info("Drop queue for [" + tableName + "]: 0");
			    } else {
				LOG.info("Drop queue for [" + tableName + "]: " + dropList.length);
				dropQueue(dropList, oldRules);
			    }
			} finally {
			    QUEUE_LOCK.writeLock().unlock();
			}
		    }
		}
	    } finally {
		REFERENCE_COUNTER_LOCK.unlock();
	    }
	}

	private void addQueue(String[] addList, IncrementRules newRules) {
	    Map<String, BlockingQueue<IncrementMessage>> queueMap = (Map<String, BlockingQueue<IncrementMessage>>) sharedData.get(queue_key);
	    Map<String, Map<String, AsyncWriterThread>> queueWritersMap = (Map<String, Map<String, AsyncWriterThread>>) sharedData.get(writer_key);

	    if (null != queueMap && null != queueWritersMap) {
		Map<String, Integer> queueSizeMap = newRules.getQueueSizeMap();
		for (String queueName : addList) {
		    int queueSize = queueSizeMap.get(queueName);
		    BlockingQueue<IncrementMessage> queue = new ArrayBlockingQueue<IncrementMessage>(queueSize);
		    int writerCount = newRules.getWriterCount(queueName);

		    Configuration newConf = HBaseConfiguration.create(conf);
		    newConf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, (int) newRules.getRpcTimeout(queueName));
		    newConf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, (int) newRules.getRpcTimeout(queueName));
		    // newConf.setInt(HConstants.HBASE_CLIENT_RPC_MAXATTEMPTS,
		    // newRules.getClientRpcMaxAttempts(queueName));
		    newConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, newRules.getClientRetriesNum(queueName));

		    Map<String, AsyncWriterThread> writersMap = new HashMap<String, AsyncWriterThread>();
		    for (int i = 0; i < writerCount; ++i) {
			String writerName = "Async Writer Thread " + i + " for [" + tableName + "=>" + queueName + "]";
			AsyncWriterThread writer = new AsyncWriterThread(newConf, rce, queue, queueName);
			writer.setName(writerName);
			writer.setDaemon(true);
			writer.start();
			writersMap.put(writerName, writer);
		    }

		    queueMap.put(queueName, queue);
		    queueWritersMap.put(queueName, writersMap);
		}
	    }
	}

	private void changeQueue(String[] changeList, IncrementRules newRules, IncrementRules oldRules) {
	    Map<String, BlockingQueue<IncrementMessage>> queueMap = (Map<String, BlockingQueue<IncrementMessage>>) sharedData.get(queue_key);
	    Map<String, Map<String, AsyncWriterThread>> queueWritersMap = (Map<String, Map<String, AsyncWriterThread>>) sharedData.get(writer_key);

	    if (null != queueMap && null != queueWritersMap) {
		Map<String, Integer> queueSizeMap = newRules.getQueueSizeMap();
		for (String queueName : changeList) {
		    Map<String, AsyncWriterThread> writersMap = queueWritersMap.remove(queueName);
		    if (null != writersMap) {
			for (Entry<String, AsyncWriterThread> entry : writersMap.entrySet()) {
			    entry.getValue().interrupt();
			}
		    }

		    BlockingQueue<IncrementMessage> oldQueue = queueMap.remove(queueName);

		    int queueSize = queueSizeMap.get(queueName);
		    BlockingQueue<IncrementMessage> newQueue = new ArrayBlockingQueue<IncrementMessage>(queueSize);
		    if (null != oldQueue) {
			oldQueue.drainTo(newQueue, queueSize);

			COUNTERS_LOCK.readLock().lock();
			try {
			    Counters counters = (Counters) sharedData.get(counters_key);
			    if (null != counters) {
				counters.increaseDrop(queueName, oldQueue.size());
			    }
			} finally {
			    COUNTERS_LOCK.readLock().unlock();
			}

			oldQueue.clear();
		    }

		    int writerCount = newRules.getWriterCount(queueName);

		    Configuration newConf = HBaseConfiguration.create(conf);
		    newConf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, (int) newRules.getRpcTimeout(queueName));
		    newConf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, (int) newRules.getRpcTimeout(queueName));
		    // newConf.setInt(HConstants.HBASE_CLIENT_RPC_MAXATTEMPTS,
		    // newRules.getClientRpcMaxAttempts(queueName));
		    newConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, newRules.getClientRetriesNum(queueName));

		    writersMap = new HashMap<String, AsyncWriterThread>();
		    for (int i = 0; i < writerCount; ++i) {
			String writerName = "Async Writer Thread " + i + " for [" + tableName + "=>" + queueName + "]";
			AsyncWriterThread writer = new AsyncWriterThread(newConf, rce, newQueue, queueName);
			writer.setName(writerName);
			writer.setDaemon(true);
			writer.start();
			writersMap.put(writerName, writer);
		    }

		    queueMap.put(queueName, newQueue);
		    queueWritersMap.put(queueName, writersMap);
		}
	    }
	}

	private void dropQueue(String[] dropList, IncrementRules oldRules) {
	    Map<String, BlockingQueue<IncrementMessage>> queueMap = (Map<String, BlockingQueue<IncrementMessage>>) sharedData.get(queue_key);
	    if (null != queueMap) {
		for (String queueName : dropList) {
		    BlockingQueue<IncrementMessage> queue = queueMap.remove(queueName);
		    if (null != queue) {
			COUNTERS_LOCK.readLock().lock();
			try {
			    Counters counters = (Counters) sharedData.get(counters_key);
			    if (null != counters) {
				counters.increaseDrop(queueName, queue.size());
			    }
			} finally {
			    COUNTERS_LOCK.readLock().unlock();
			}

			queue.clear();
		    }
		}
	    }

	    Map<String, Map<String, AsyncWriterThread>> queueWritersMap = (Map<String, Map<String, AsyncWriterThread>>) sharedData.get(writer_key);
	    if (null != queueWritersMap) {
		for (String queueName : dropList) {
		    Map<String, AsyncWriterThread> writersMap = queueWritersMap.remove(queueName);
		    if (null != writersMap) {
			for (Entry<String, AsyncWriterThread> entry : writersMap.entrySet()) {
			    entry.getValue().interrupt();
			}
		    }
		}
	    }
	}
    }

    class AsyncWriterThread extends Thread {
	private Configuration conf;
	private RegionCoprocessorEnvironment rce;
	private Map<String, Object> sharedData;
	private BlockingQueue<IncrementMessage> queue;
	private String queueName;
	private HQueue target;
	private int requeueNum;

	public AsyncWriterThread(Configuration conf, RegionCoprocessorEnvironment rce, BlockingQueue<IncrementMessage> queue, String queueName) {
	    this.conf = conf;
	    this.rce = rce;
	    this.queue = queue;
	    this.queueName = queueName;
	}

	private boolean initialize() {
	    LOG.info("[" + this.getName() + "] starts running...");
	    sharedData = rce.getSharedData();
	    requeueNum = 0;

	    try {
		target = new HQueue(conf, queueName);
		target.setAutoFlush(true);
	    } catch (Exception e) {
		if (null != target) {
		    try {
			target.close();
		    } catch (IOException ioe) {
			LOG.warn(ioe.toString());
		    }
		}

		LOG.warn(e.toString());
		return false;
	    }

	    return true;
	}

	public void run() {
	    if (!initialize()) {
		return;
	    }

	    int batchSize = -1;
	    int requeueMaxNum = -1;
	    double retryMaxRatio = -1;
	    long waitInterval = -1;
	    long emptyInterval = -1;
	    while (!Thread.currentThread().isInterrupted()) {
		RULES_LOCK.readLock().lock();
		try {
		    IncrementRules rules = (IncrementRules) sharedData.get(rules_key);
		    if (null == rules) {
			break;
		    }

		    batchSize = rules.getWriterBatchSize(queueName);
		    requeueMaxNum = rules.getRequeueMaxNum(queueName);
		    retryMaxRatio = rules.getRetryMaxRatio(queueName);
		    waitInterval = rules.getWriterWaitInterval(queueName);
		    emptyInterval = rules.getWriterEmptyInterval(queueName);
		} finally {
		    RULES_LOCK.readLock().unlock();
		}

		List<IncrementMessage> batchList = new ArrayList<IncrementMessage>(batchSize);
		int count = queue.drainTo(batchList, batchSize);

		// if the queue is empty, sleep some time and retry
		if (0 == count) {
		    try {
			sleep(emptyInterval);
		    } catch (InterruptedException e) {
			break;
		    }

		    continue;
		}

		// if the size of queue is not enough for the buffer, sleep some
		// time and give another try, then process the batch anyway
		if (count < batchSize) {
		    try {
			sleep(waitInterval);
		    } catch (InterruptedException e) {
			// interrupt() method was called, so process
			// buffered messages before stopping
			process(batchList, requeueMaxNum, retryMaxRatio);
			break;
		    }

		    int leftRoom = batchSize - count;
		    queue.drainTo(batchList, leftRoom);
		}

		process(batchList, requeueMaxNum, retryMaxRatio);
	    }

	    clear();
	}

	private void process(List<IncrementMessage> messages, int requeueMaxNum, double retryMaxRatio) {
	    List<Message> puts = new LinkedList<Message>();

	    try {
		short partitionCount = target.getPartitionCount();
		for (IncrementMessage m : messages) {
		    Message message = m.getTargetMessage();
		    String rowkey = getRowkey(message);
		    if (null != rowkey) {
			String md5Str = CoprocessorUtils.toMD5String(rowkey);
			if (null != md5Str) {
			    int hash = md5Str.hashCode();
			    message.setPartitionID((short) ((0 > hash ? -hash : hash) % partitionCount));
			}
		    }

		    puts.add(message);
		}

		target.put(puts);
		requeueNum = 0;
		COUNTERS_LOCK.readLock().lock();
		try {
		    Counters counters = (Counters) sharedData.get(counters_key);
		    if (null != counters) {
			counters.increasePush(queueName, puts.size());
		    }
		} finally {
		    COUNTERS_LOCK.readLock().unlock();
		}
	    } catch (Throwable e) {
		double ratio = (double) queue.size() / (queue.size() + queue.remainingCapacity());
		if (ratio > retryMaxRatio) {
		    LOG.warn("Putting to [" + queueName + "] failed. Because the buffer was almost full, dropped " + puts.size() + " put(s).", e);
		    COUNTERS_LOCK.readLock().lock();
		    try {
			Counters counters = (Counters) sharedData.get(counters_key);
			if (null != counters) {
			    counters.increaseDrop(queueName, puts.size());
			}
		    } finally {
			COUNTERS_LOCK.readLock().unlock();
		    }
		} else if (requeueNum > requeueMaxNum) {
		    LOG.warn("Putting to [" + queueName + "] failed. Because it reached requeue max number limit (" + requeueMaxNum + "), dropped " + puts.size() + " put(s).", e);
		    COUNTERS_LOCK.readLock().lock();
		    try {
			Counters counters = (Counters) sharedData.get(counters_key);
			if (null != counters) {
			    counters.increaseDrop(queueName, puts.size());
			}
		    } finally {
			COUNTERS_LOCK.readLock().unlock();
		    }
		} else {
		    LOG.warn("Trying to requeue [" + puts.size() + "] message(s) back to [" + queueName + "] buffer...", e);
		    int done = 0;
		    for (IncrementMessage message : messages) {
			if (queue.offer(message)) {
			    done++;
			} else {
			    break;
			}
		    }

		    int failNum = messages.size() - done;
		    if (0 < failNum) {
			COUNTERS_LOCK.readLock().lock();
			try {
			    Counters counters = (Counters) sharedData.get(counters_key);
			    if (null != counters) {
				counters.increaseDrop(queueName, failNum);
			    }
			} finally {
			    COUNTERS_LOCK.readLock().unlock();
			}

			if (0 < done) {
			    // partly successful
			    requeueNum++;
			}
		    } else {
			// total successful
			requeueNum++;
		    }

		    LOG.warn("Buffer [" + queueName + "]: requeued (" + done + "), dropped (" + failNum + ")");
		}
	    }
	}

	private String getRowkey(Message message) {
	    if (null == message || null == message.getValue()) {
		return null;
	    }

	    String content = Bytes.toString(message.getValue());
	    String rowkey = null;
	    try {
		JSONObject json = new JSONObject(content);
		if (json.has("rowkey")) {
		    rowkey = json.getString("rowkey");
		}
	    } catch (JSONException e) {
		rowkey = content;
	    }

	    return rowkey;
	}

	private void clear() {
	    try {
		if (null != target) {
		    target.close();
		}
	    } catch (IOException e) {
		LOG.warn(e.toString());
	    }

	    LOG.info("[" + this.getName() + "] is stopped.");
	}
    }

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
	RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment) e;
	Configuration conf = rce.getConfiguration();
	Map<String, Object> sharedData = rce.getSharedData();
	tableName = Bytes.toString(rce.getRegion().getRegionInfo().getTableName());
	regionName = rce.getRegion().getRegionInfo().getRegionNameAsString();
	rules_key = tableName + "_rules";
	reload_timer_key = tableName + "_reload_timer";
	monitor_timer_key = tableName + "_monitor_timer";
	reference_counter_key = tableName + "_reference_counter";
	counters_key = tableName + "_counters";
	queue_key = tableName + "_queue";
	writer_key = tableName + "_writer";

	REFERENCE_COUNTER_LOCK.lock();
	try {
	    LOG.info(COPROCESSOR_NAME + " starts on [" + tableName + ":" + regionName + "].");

	    AtomicInteger ai = (AtomicInteger) sharedData.get(reference_counter_key);
	    if (null == ai) {
		// initialize reference counter
		ai = new AtomicInteger(0);
		sharedData.put(reference_counter_key, ai);
		LOG.info("Initialize reference counter for [" + tableName + "].");

		// initialize counters
		Counters counters = new Counters();
		sharedData.put(counters_key, counters);
		LOG.info("Initialize counters for [" + tableName + "].");

		// initialize rules
		String rulesPath = conf.get(CONF_KEY_RULES_DIR) + "/" + tableName + ".json";
		String jsonStr = FileUtils.readContentFromHdfs(conf, rulesPath);
		IncrementRules rules = IncrementRules.reload(jsonStr);
		if (rules.needReload()) {
		    if (null == jsonStr || jsonStr.trim().isEmpty()) {
			LOG.warn("The rules file [" + rulesPath + "] doesn't exist or it's empty, skip it!");
		    } else {
			LOG.warn("The rules file [" + rulesPath + "] is illegal, skip it!");
		    }
		} else {
		    LOG.info("Initialize coprocessor rules for [" + tableName + "].");
		}

		sharedData.put(rules_key, rules);

		// initialize blocking queues and async writer threads
		Map<String, BlockingQueue<IncrementMessage>> queueMap = new HashMap<String, BlockingQueue<IncrementMessage>>();
		Map<String, Map<String, AsyncWriterThread>> queueWritersMap = new HashMap<String, Map<String, AsyncWriterThread>>();
		Map<String, Integer> queueSizeMap = rules.getQueueSizeMap();
		for (Entry<String, Integer> entry : queueSizeMap.entrySet()) {
		    String queueName = entry.getKey();
		    int queueSize = entry.getValue();
		    BlockingQueue<IncrementMessage> queue = new ArrayBlockingQueue<IncrementMessage>(queueSize);
		    int writerCount = rules.getWriterCount(queueName);

		    Configuration newConf = HBaseConfiguration.create(conf);
		    newConf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, (int) rules.getRpcTimeout(queueName));
		    newConf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, (int) rules.getRpcTimeout(queueName));
		    // newConf.setInt(HConstants.HBASE_CLIENT_RPC_MAXATTEMPTS,
		    // rules.getClientRpcMaxAttempts(queueName));
		    newConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, rules.getClientRetriesNum(queueName));

		    Map<String, AsyncWriterThread> writersMap = new HashMap<String, AsyncWriterThread>();
		    for (int i = 0; i < writerCount; ++i) {
			String writerName = "Async Writer Thread " + i + " for [" + tableName + "=>" + queueName + "]";
			AsyncWriterThread writer = new AsyncWriterThread(newConf, rce, queue, queueName);
			writer.setName(writerName);
			writer.setDaemon(true);
			writer.start();
			writersMap.put(writerName, writer);
		    }

		    queueMap.put(queueName, queue);
		    queueWritersMap.put(queueName, writersMap);
		}

		sharedData.put(queue_key, queueMap);
		sharedData.put(writer_key, queueWritersMap);
		LOG.info("Initialize blocking queues and async writer threads for [" + tableName + "].");

		// initialize monitor timer
		long monitorDelay = conf.getLong(CONF_KEY_MONITOR_DELAY, DEFAULT_MONITOR_DELAY);
		long monitorPeriod = conf.getLong(CONF_KEY_MONITOR_PERIOD, DEFAULT_MONITOR_PERIOD);
		Timer monitorTimer = new Timer(COPROCESSOR_NAME + " Monitor Timer - " + tableName);
		monitorTimer.schedule(new MonitorTimerTask(conf, rce), monitorDelay, monitorPeriod);
		sharedData.put(monitor_timer_key, monitorTimer);
		LOG.info("Initialize monitor timer for [" + tableName + "]: [delay=" + monitorDelay + ", period=" + monitorPeriod + "].");

		// initialize reload timer
		long reloadDelay = conf.getLong(CONF_KEY_RULES_RELOAD_DELAY, DEFAULT_RULES_RELOAD_DELAY);
		long reloadPeriod = conf.getLong(CONF_KEY_RULES_RELOAD_PERIOD, DEFAULT_RULES_RELOAD_PERIOD);
		Timer reloadTimer = new Timer(COPROCESSOR_NAME + " Rules Reload Timer - " + tableName);
		reloadTimer.schedule(new RulesReloadTimerTask(conf, rce), reloadDelay, reloadPeriod);
		sharedData.put(reload_timer_key, reloadTimer);
		LOG.info("Initialize rules reload timer for [" + tableName + "]: [delay=" + reloadDelay + ", period=" + reloadPeriod + "].");
	    }

	    ai.incrementAndGet();
	} finally {
	    REFERENCE_COUNTER_LOCK.unlock();
	}
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
	RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment) e;
	Map<String, Object> sharedData = rce.getSharedData();

	REFERENCE_COUNTER_LOCK.lock();
	try {
	    LOG.info(COPROCESSOR_NAME + " stops on [" + tableName + ":" + regionName + "].");

	    AtomicInteger ai = (AtomicInteger) sharedData.get(reference_counter_key);
	    if (null != ai) {
		if (0 == ai.decrementAndGet()) {
		    // release reload timer
		    LOG.info("Release rules reload timer for [" + tableName + "].");
		    Timer reloadTimer = (Timer) sharedData.remove(reload_timer_key);
		    if (null != reloadTimer) {
			reloadTimer.cancel();
		    }

		    // release monitor timer
		    LOG.info("Release monitor timer for [" + tableName + "].");
		    Timer monitorTimer = (Timer) sharedData.remove(monitor_timer_key);
		    if (null != monitorTimer) {
			monitorTimer.cancel();
		    }

		    // release rules
		    RULES_LOCK.writeLock().lock();
		    try {
			LOG.info("Release coprocessor rules for [" + tableName + "].");
			sharedData.remove(rules_key);
		    } finally {
			RULES_LOCK.writeLock().unlock();
		    }

		    // release blocking queues and async writer threads
		    QUEUE_LOCK.writeLock().lock();
		    try {
			LOG.info("Release blocking queues for [" + tableName + "].");
			Map<String, BlockingQueue<IncrementMessage>> queueMap = (Map<String, BlockingQueue<IncrementMessage>>) sharedData.remove(queue_key);
			if (null != queueMap) {
			    for (Entry<String, BlockingQueue<IncrementMessage>> entry : queueMap.entrySet()) {
				COUNTERS_LOCK.readLock().lock();
				try {
				    Counters counters = (Counters) sharedData.get(counters_key);
				    if (null != counters) {
					counters.increaseDrop(entry.getKey(), entry.getValue().size());
				    }
				} finally {
				    COUNTERS_LOCK.readLock().unlock();
				}

				entry.getValue().clear();
			    }
			}

			LOG.info("Release async writer threads for [" + tableName + "].");
			Map<String, Map<String, AsyncWriterThread>> queueWritersMap = (Map<String, Map<String, AsyncWriterThread>>) sharedData.remove(writer_key);
			if (null != queueWritersMap) {
			    for (Entry<String, Map<String, AsyncWriterThread>> entry : queueWritersMap.entrySet()) {
				for (Entry<String, AsyncWriterThread> subEntry : entry.getValue().entrySet()) {
				    subEntry.getValue().interrupt();
				}
			    }
			}
		    } finally {
			QUEUE_LOCK.writeLock().unlock();
		    }

		    // release counters
		    COUNTERS_LOCK.writeLock().lock();
		    try {
			LOG.info("Release counters for [" + tableName + "].");
			sharedData.remove(counters_key);
		    } finally {
			COUNTERS_LOCK.writeLock().unlock();
		    }

		    // release reference counter
		    LOG.info("Release reference counter for [" + tableName + "].");
		    sharedData.remove(reference_counter_key);
		}
	    }
	} finally {
	    REFERENCE_COUNTER_LOCK.unlock();
	}
    }

    @Override
    public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
	RegionCoprocessorEnvironment rce = c.getEnvironment();
	Map<String, Object> sharedData = rce.getSharedData();
	Map<String, List<IncrementMessage>> queueMessagesMap = new HashMap<String, List<IncrementMessage>>();
	long offerTimeout = 0;

	RULES_LOCK.readLock().lock();
	try {
	    IncrementRules rules = (IncrementRules) sharedData.get(rules_key);
	    if (null == rules) {
		return;
	    }

	    for (int i = 0; i < miniBatchOp.size(); i++) {
		if (miniBatchOp.getOperationStatus(i).getOperationStatusCode() != OperationStatusCode.SUCCESS) {
		    continue;
		}

		Mutation mutation = miniBatchOp.getOperation(i);
		if (mutation instanceof Put) {
		    Put put = (Put) mutation;
		    List<IncrementMessage> messages = rules.apply(put);
		    if (null != messages) {
			for (IncrementMessage m : messages) {
			    String queueName = m.getQueueName();
			    List<IncrementMessage> messageList = queueMessagesMap.get(queueName);
			    if (null == messageList) {
				messageList = new LinkedList<IncrementMessage>();
				queueMessagesMap.put(queueName, messageList);
			    }

			    messageList.add(m);
			}
		    }
		}
	    }

	    offerTimeout = rules.getOfferTimeout();
	} finally {
	    RULES_LOCK.readLock().unlock();
	}

	QUEUE_LOCK.readLock().lock();
	try {
	    Map<String, BlockingQueue<IncrementMessage>> queueMap = (Map<String, BlockingQueue<IncrementMessage>>) sharedData.get(queue_key);
	    for (Entry<String, List<IncrementMessage>> entry : queueMessagesMap.entrySet()) {
		String queueName = entry.getKey();
		BlockingQueue<IncrementMessage> queue = queueMap.get(queueName);
		if (null == queue) {
		    LOG.warn("Unknown queue name \"" + queueName + "\" for [" + tableName + "]!");
		    continue;
		}

		int done = 0;
		long startTime = EnvironmentEdgeManager.currentTime();
		List<IncrementMessage> messages = entry.getValue();
		for (IncrementMessage m : messages) {
		    try {
			if (queue.offer(m, offerTimeout, TimeUnit.MILLISECONDS)) {
			    done++;
			}
		    } catch (InterruptedException e) {
			LOG.warn(e.toString());
		    }

		    long totalTime = EnvironmentEdgeManager.currentTime() - startTime;
		    if (totalTime > offerTimeout) {
			LOG.warn("[" + tableName + ":" + regionName + "] " + totalTime + " ms, offer " + done + " message(s) into [" + queueName + "], drop "
				+ (messages.size() - done) + " message(s).");
			break;
		    }
		}

		COUNTERS_LOCK.readLock().lock();
		try {
		    Counters counters = (Counters) sharedData.get(counters_key);
		    if (null != counters) {
			counters.increaseDrop(queueName, messages.size() - done);
		    }
		} finally {
		    COUNTERS_LOCK.readLock().unlock();
		}
	    }
	} finally {
	    QUEUE_LOCK.readLock().unlock();
	}
    }
    
    public static void main(String[] args) throws Exception {
	if (2 != args.length) {
	    System.err.println("Usage: exec <table_name> <data_path>");
	    System.exit(1);
	}

	String tableName = args[0];
	String dataPath = args[1];
	
	Configuration conf = HBaseConfiguration.create();
	System.out.println("Zookeeper: [" + conf.get(HConstants.ZOOKEEPER_QUORUM) + "]");
	System.out.println("Inintialize htable [" + tableName + "]...");
	HTable table = new HTable(conf, tableName);
	table.setAutoFlush(false);
	
	System.out.println("Read data from file [" + dataPath + "]...");
	String jsonArrStr = null;
	if (dataPath.startsWith("hdfs://")) {
	    jsonArrStr = FileUtils.readContentFromHdfs(conf, dataPath);
	} else {
	    jsonArrStr = FileUtils.readContentFromLocal(dataPath);
	}

	List<Put> puts = new LinkedList<Put>();
	JSONArray jsonArr = new JSONArray(jsonArrStr);
	for (int i = 0; i < jsonArr.length(); ++i) {
	    JSONObject json = jsonArr.getJSONObject(i);
	    String rowkey = json.getString("rowkey");
	    Put put = new Put(Bytes.toBytes(rowkey));
	    System.out.println("Put to [" + rowkey + "]:");

	    JSONObject contentJson = json.getJSONObject("content");
	    Iterator iterJson = contentJson.keys();
	    while (iterJson.hasNext()) {
		String familyName = (String) iterJson.next();
		JSONObject familyJson = contentJson.getJSONObject(familyName);
		Iterator iterFamilyJson = familyJson.keys();
		while (iterFamilyJson.hasNext()) {
		    String qualifierName = (String) iterFamilyJson.next();
		    String value = familyJson.getString(qualifierName);
		    put.add(Bytes.toBytes(familyName), Bytes.toBytes(qualifierName), Bytes.toBytes(value));
		    System.out.println("Add [" + familyName + ":" + qualifierName + "=" + value + "];");
		}
	    }

	    puts.add(put);
	}

	System.out.println("Do put: " + puts.size() + " item(s).");
	table.put(puts);
	table.close();

	System.exit(0);
    }
}
