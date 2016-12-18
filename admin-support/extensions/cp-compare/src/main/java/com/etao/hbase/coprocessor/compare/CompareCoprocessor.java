package com.etao.hbase.coprocessor.compare;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.etao.hbase.coprocessor.compare.rule.Action;
import com.etao.hbase.coprocessor.compare.rule.CompareRule;
import com.etao.hbase.coprocessor.compare.rule.Operator;
import com.etao.hbase.coprocessor.compare.rule.Trigger;
import com.etao.hbase.coprocessor.util.FileUtils;

/**
 * 
 * @author lingbo.zm
 * 
 */
public class CompareCoprocessor extends BaseRegionObserver {
    private static final Log LOG = LogFactory.getLog(CompareCoprocessor.class);

    private static final String CONF_KEY_RULES = "rules";
    private static final String CONF_KEY_RULES_RELOAD_DELAY = "rules.reload.delay";
    private static final String CONF_KEY_RULES_RELOAD_PERIOD = "rules.reload.period";
    private static final String COPROCESSOR_NAME = "CompareCoprocessor";

    private static final long DEFAULT_RULES_RELOAD_DELAY = 180000;
    private static final long DEFAULT_RULES_RELOAD_PERIOD = 900000;

    private static final String JSON_COLUMN = "column";
    private static final String JSON_TRIGGER = "trigger";
    private static final String JSON_TRIGGER_OPERATOR = "operator";
    private static final String JSON_TRIGGER_VALUE = "value";
    private static final String JSON_ACTION = "action";
    private static final String JSON_ACTION_COLUMN = "column";
    private static final String JSON_ACTION_VALUE = "value";

    private static final String SELF = "${self}";

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
    			String jsonSource = FileUtils.readContentFromHdfs(conf, conf.get(CONF_KEY_RULES));
    			List<CompareRule> rules = parse(jsonSource);
    			sharedData.put(rules_key, rules);

    			// initialize reload timer
    			long delay = conf.getLong(CONF_KEY_RULES_RELOAD_DELAY, DEFAULT_RULES_RELOAD_DELAY);
    			long period = conf.getLong(CONF_KEY_RULES_RELOAD_PERIOD, DEFAULT_RULES_RELOAD_PERIOD);

    			LOG.info("initialize rules reload timer : [delay=" + delay + ", period=" + period + "]");
    			Timer reloadTimer = new Timer("Compare Rules Reload Timer");
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

    private List<CompareRule> parse(String jsonSource) {
    	List<CompareRule> rules = new ArrayList<CompareRule>();

		try {
		    JSONArray rulesJson = new JSONArray(jsonSource);
		    for (int i = 0; i < rulesJson.length(); i++) {
				CompareRule rule = new CompareRule();
		
				JSONObject ruleJson = rulesJson.getJSONObject(i);
				rule.setColumn(ruleJson.getString(JSON_COLUMN));
		
				JSONObject triggerJson = ruleJson.getJSONObject(JSON_TRIGGER);
				Operator operator = null;
				try {
				    operator = Operator.valueOf(triggerJson.getString(JSON_TRIGGER_OPERATOR));
				} catch (IllegalArgumentException e) {
				    LOG.warn("unknown operator '" + triggerJson.getString(JSON_TRIGGER_OPERATOR) + "'", e);
				    continue;
				}
		
				Trigger trigger = new Trigger();
				trigger.setOperator(operator);
				trigger.setValue(triggerJson.getString(JSON_TRIGGER_VALUE));
				rule.setTrigger(trigger);
		
				JSONObject actionJson = ruleJson.getJSONObject(JSON_ACTION);
				Action action = new Action();
				action.setColumn(actionJson.getString(JSON_ACTION_COLUMN));
				action.setValue(actionJson.getString(JSON_ACTION_VALUE));
				rule.setAction(action);
		
				rules.add(rule);
		    }
		} catch (JSONException e) {
		    LOG.error("cannot parse the json '" + jsonSource + "'", e);
		}
	
		return rules;
    }

    /**
     * Timer for reloading compare coprocessor rules
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
	
				String jsonSource = FileUtils.readContentFromHdfs(conf, conf.get(CONF_KEY_RULES));
				if (jsonSource == null || jsonSource.isEmpty()) {
				    LOG.warn("the rules file '" + conf.get(CONF_KEY_RULES) + "' doesn't exist, or it is empty, skip it!");
				    return;
				}
		
				List<CompareRule> oldRules = null;
				List<CompareRule> rules = parse(jsonSource);

				RULES_RW_LOCK.writeLock().lock();
				try {
				    oldRules = (List<CompareRule>) sharedData.get(rules_key);
				    if (null == oldRules) {
				    	LOG.warn("the old rules for [" + tableName + "] had already been released, skip it!");
				    	return;
				    }
		
				    sharedData.put(rules_key, rules);
		
				    LOG.info("reloaded compare coprocessor rules: " + rules);
				} finally {
				    RULES_RW_LOCK.writeLock().unlock();
				}
		    } finally {
		    	REFERENCE_COUNTER_LOCK.unlock();
		    }
    	}
    }

    @Override
    @SuppressWarnings("unchecked")
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
    	RegionCoprocessorEnvironment rce = e.getEnvironment();
    	Map<String, Object> sharedData = rce.getSharedData();
    	
    	Map<byte[], List<Cell>> familyMap = put.getFamilyCellMap();

    	RULES_RW_LOCK.readLock().lock();
    	try {
    		List<CompareRule> rules = (List<CompareRule>) sharedData.get(rules_key);
    		if (null == rules || rules.isEmpty()) {
    			return;
    		}

    		for (CompareRule rule : rules) {
    			byte[] family = Bytes.toBytes(rule.getFamily());
    			List<Cell> kvs = familyMap.get(family);
    			if (kvs == null) {
    				continue;
    			}
		
    			for (Cell kv : kvs) {
    			    String qualifier = Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength());
    				if (!rule.matchesQualifier(qualifier)) {
    					continue;
    				}
    				
    				byte[] qualifierBytes = Bytes.toBytes(qualifier);

    				String sourceValue = Bytes.toString(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
		    
    				Trigger trigger = rule.getTrigger();
    				String destValue = trigger.getValue();
    				if (SELF.equals(destValue)) {
    					Get get = new Get(put.getRow());
    					get.addColumn(family, qualifierBytes);
    					Result result = e.getEnvironment().getRegion().get(get);
    					if (result == null || result.isEmpty()) {
    						destValue = null;
    					} else {
    						Cell latestCell = result.getColumnLatestCell(family, qualifierBytes);
    						destValue = Bytes.toString(latestCell.getValueArray(), latestCell.getValueOffset(), latestCell.getValueLength());
    					}
    				}

    				if (isTriggered(sourceValue, trigger.getOperator(), destValue)) {
    					Action action = rule.getAction();
    					byte[][] bytes = KeyValue.parseColumn(Bytes.toBytes(action.getColumn()));
    					put.add(bytes[0], bytes[1], Bytes.toBytes(action.getValue()));
    				}

    				if (rule.isExactMatch()) {
    					break;
    				}
    			}
    		}
    	} finally {
    		RULES_RW_LOCK.readLock().unlock();
    	}
    }

    private boolean isTriggered(String sourceValue, Operator operator, String destValue) {
    	boolean triggered = false;
    	switch (operator) {
    	case NEQ:
    		if (destValue != null) {
				try {
				    float sourceVal = Float.parseFloat(sourceValue);
				    float destVal = Float.parseFloat(destValue);
		
				    triggered = sourceVal == destVal;
				} catch (NumberFormatException e) {
				}
    		}
    		break;
    	case NNE:
    		if (destValue == null) {
    			triggered = true;
    		} else {
    			try {
    				float sourceVal = Float.parseFloat(sourceValue);
    				float destVal = Float.parseFloat(destValue);
		    
    				triggered = sourceVal != destVal;
    			} catch (NumberFormatException e) {
    			}
    		}
    		break;
    	case NLT:
    		if (destValue != null) {
				try {
				    float sourceVal = Float.parseFloat(sourceValue);
				    float destVal = Float.parseFloat(destValue);
		
				    triggered = sourceVal < destVal;
				} catch (NumberFormatException e) {
				}
    		}
    		break;
    	case NLE:
    		if (destValue != null) {
    			try {
    				float sourceVal = Float.parseFloat(sourceValue);
    				float destVal = Float.parseFloat(destValue);

    				triggered = sourceVal <= destVal;
    			} catch (NumberFormatException e) {
    			}
    		}
    		break;
    	case NGT:
    		if (destValue != null) {
    			try {
    				float sourceVal = Float.parseFloat(sourceValue);
    				float destVal = Float.parseFloat(destValue);
		    
    				triggered = sourceVal > destVal;
    			} catch (NumberFormatException e) {
    			}
    		}
    		break;
    	case NGE:
    		if (destValue != null) {
				try {
				    float sourceVal = Float.parseFloat(sourceValue);
				    float destVal = Float.parseFloat(destValue);
		
				    triggered = sourceVal >= destVal;
				} catch (NumberFormatException e) {
				}
    		}
    		break;
		case SEQ:
		    if (destValue != null) {
		    	triggered = sourceValue.equals(destValue);
		    }
		    break;
		case SNE:
		    if (destValue == null) {
		    	triggered = true;
		    } else {
		    	triggered = !sourceValue.equals(destValue);
		    }
		    break;
		default:
			break;
    	}
	
    	return triggered;
    }
}
