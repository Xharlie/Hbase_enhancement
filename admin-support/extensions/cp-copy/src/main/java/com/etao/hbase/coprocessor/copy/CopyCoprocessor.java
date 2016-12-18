/**
 * 
 */
package com.etao.hbase.coprocessor.copy;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.json.JSONArray;
import org.json.JSONObject;

import com.etao.hbase.coprocessor.copy.common.CopyCoprocessorConstants;
import com.etao.hbase.coprocessor.copy.rule.CopyRule;
import com.etao.hbase.coprocessor.util.FileUtils;

/**
 * CopyCoprocessor copy column value and store it into another column when it
 * satisfies the conditions
 * 
 * @author lingbo.zm
 * 
 */
public class CopyCoprocessor extends BaseRegionObserver {
    private static final Log LOG = LogFactory.getLog(CopyCoprocessor.class);

    private static final String COPROCESSOR_NAME = "CopyCoprocessor";

    private static ReadWriteLock RULES_RW_LOCK = new ReentrantReadWriteLock();
    private static ReentrantLock REFERENCE_COUNTER_LOCK = new ReentrantLock();

    private String tableName;
    private String regionName;
    private String rules_key;
    private String reload_timer_key;
    private String reference_counter_key;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
		RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment) e;
		Configuration conf = rce.getConfiguration();
		Map<String, Object> sharedData = rce.getSharedData();
	
		tableName = rce.getRegion().getRegionInfo().getTable().getNameAsString();
		regionName = rce.getRegion().getRegionInfo().getRegionNameAsString();
	
		rules_key = tableName + "_rules";
		reload_timer_key = tableName + "_reload_timer";
		reference_counter_key = tableName + "_reference_counter";

		REFERENCE_COUNTER_LOCK.lock();
		try {
		    LOG.info(COPROCESSOR_NAME + " starts on [" + tableName + ":" + regionName + "].");
	
		    AtomicInteger ai = (AtomicInteger) sharedData.get(reference_counter_key);
		    if (null == ai) {
		    	// initialize reference counter
		    	LOG.info("initialize reference counter for [" + tableName + "].");
		    	ai = new AtomicInteger(0);
		    	sharedData.put(reference_counter_key, ai);
	
		    	// initialize rules
		    	String jsonSource = FileUtils.readContentFromHdfs(conf, conf.get(CopyCoprocessorConstants.CONF_KEY_RULES));
				List<CopyRule> rules = parseRules(jsonSource);
				sharedData.put(rules_key, rules);
		
				// initialize reload timer
				long delay = conf.getLong(CopyCoprocessorConstants.CONF_KEY_RULES_RELOAD_DELAY, CopyCoprocessorConstants.DEFAULT_RULES_RELOAD_DELAY);
				long period = conf.getLong(CopyCoprocessorConstants.CONF_KEY_RULES_RELOAD_PERIOD, CopyCoprocessorConstants.DEFAULT_RULES_RELOAD_PERIOD);
		
				LOG.info("initialize rules reload timer : [delay=" + delay + ", period=" + period + "]");
				Timer reloadTimer = new Timer("Copy Rules Reload Timer");
				reloadTimer.schedule(new RulesReloadTimerTask(conf, rce), delay, period);
				sharedData.put(reload_timer_key, reloadTimer);
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
				    LOG.info("release rules reload timer for [" + tableName + "].");
				    Timer reloadTimer = (Timer) sharedData.remove(reload_timer_key);
				    if (null != reloadTimer) {
				    	reloadTimer.cancel();
				    }
		
				    // release rules
				    RULES_RW_LOCK.writeLock().lock();
				    try {
				    	LOG.info("release coprocessor rules for [" + tableName + "].");
				    	sharedData.remove(rules_key);
				    } finally {
				    	RULES_RW_LOCK.writeLock().unlock();
				    }
		
				    // release reference counter
				    LOG.info("release reference counter for [" + tableName + "].");
				    sharedData.remove(reference_counter_key);
				}
		    }
		} finally {
		    REFERENCE_COUNTER_LOCK.unlock();
		}
    }

    @Override
    @SuppressWarnings("unchecked")
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
    	RegionCoprocessorEnvironment rce = e.getEnvironment();
    	Map<String, Object> sharedData = rce.getSharedData();

		RULES_RW_LOCK.readLock().lock();
		try {
		    List<CopyRule> rules = (List<CopyRule>) sharedData.get(rules_key);
		    if (null == rules || rules.isEmpty()) {
		    	return;
		    }
	
		    List<Cell> kvsAdded = new LinkedList<Cell>();
	
		    for (CopyRule rule : rules) {
		    	List<Cell> kvsAddedForRule = rule.apply(put);
		    	if (kvsAddedForRule != null) {
		    		kvsAdded.addAll(kvsAddedForRule);
		    	}
		    }
	
		    for (Cell kv : kvsAdded) {
		    	put.add(kv);
		    }
		} catch (Exception ex) {
		    LOG.error("copy coprocessor preput error.", ex);
		} finally {
		    RULES_RW_LOCK.readLock().unlock();
		}
    }

    /**
     * Timer for reloading copy coprocessor rules
     * 
     * @author lingbo.zm
     * 
     */
    @SuppressWarnings("unchecked")
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
		    	LOG.info("start reloading configs for [" + tableName + "]...");
	
		    	AtomicInteger ai = (AtomicInteger) sharedData.get(reference_counter_key);
		    	if (null != ai) {
		    		LOG.info("reference count for [" + tableName + "] is: " + ai.get());
		    	}
	
		    	String jsonSource = FileUtils.readContentFromHdfs(conf, conf.get(CopyCoprocessorConstants.CONF_KEY_RULES));
		    	if (jsonSource == null || jsonSource.isEmpty()) {
		    		LOG.warn("the rules file '" + conf.get(CopyCoprocessorConstants.CONF_KEY_RULES) + "' doesn't exist, or it is empty, skip it!");
		    		return;
		    	}
	
		    	List<CopyRule> oldRules = null;
		    	List<CopyRule> rules = parseRules(jsonSource);
	
		    	RULES_RW_LOCK.writeLock().lock();
		    	try {
		    		oldRules = (List<CopyRule>) sharedData.get(rules_key);
		    		if (null == oldRules) {
		    			LOG.warn("the old rules for [" + tableName + "] had already been released, skip it!");
		    			return;
		    		}
			    
		    		sharedData.put(rules_key, rules);
			    
		    		LOG.info("reloaded copy coprocessor rules: " + rules);
		    	} finally {
		    		RULES_RW_LOCK.writeLock().unlock();
		    	}
		    } finally {
		    	REFERENCE_COUNTER_LOCK.unlock();
		    }
		}
    }

    /**
     * parse copy coprocessor rules
     * 
     * @param jsonStr
     * @return
     */
    private List<CopyRule> parseRules(String jsonStr) {
    	List<CopyRule> rules = new LinkedList<CopyRule>();
	
    	try {
    		if (null != jsonStr && !jsonStr.isEmpty()) {
    			JSONArray rulesArr = new JSONArray(jsonStr);

				for (int i = 0; i < rulesArr.length(); i++) {
				    JSONObject ruleJSON = rulesArr.getJSONObject(i);
		
				    CopyRule rule = new CopyRule(ruleJSON);
		
				    rules.add(rule);
				}
    		}
    	} catch (Exception ex) {
    		LOG.error("cannot parse copy coprocessor rules: " + jsonStr, ex);
    	}
	
    	return rules;
    }
}
