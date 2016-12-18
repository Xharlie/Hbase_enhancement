/**
 * 
 */
package com.etao.hbase.coprocessor.increment.config;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.etao.hbase.coprocessor.increment.IncrementMessage;

/**
 * @author yutian.xb
 *
 */
public class Rule {
    private static final Log LOG = LogFactory.getLog(Rule.class);

    private byte[][] trigger;
    private List<Condition> conditionList = new LinkedList<Condition>();
    private List<Event> eventList = new LinkedList<Event>();

    public String getTrigger() {
	if (null != trigger && 2 == trigger.length) {
	    return Bytes.toString(KeyValue.makeColumn(trigger[0], trigger[1]));
	}

	return null;
    }

    public List<Condition> getConditionList() {
	return this.conditionList;
    }

    public List<Event> getEventList() {
	return this.eventList;
    }

    public Rule(JSONObject json) throws JSONException {
	if (null == json) {
	    return;
	}

	if (!json.has("trigger") || !json.has("events")) {
	    return;
	}

	trigger = KeyValue.parseColumn(Bytes.toBytes(json.getString("trigger")));

	if (json.has("conditions")) {
	    JSONArray conditionsArr = json.getJSONArray("conditions");
	    for (int i = 0; i < conditionsArr.length(); ++i) {
		conditionList.add(new Condition(conditionsArr.getJSONObject(i)));
	    }
	}

	JSONArray eventsArr = json.getJSONArray("events");
	for (int i = 0; i < eventsArr.length(); ++i) {
	    eventList.add(new Event(eventsArr.getJSONObject(i)));
	}
    }

    public List<IncrementMessage> apply(Put put) {
	if (null == trigger) {
	    return null;
	}

	if (!put.has(trigger[0], trigger[1])) {
	    return null;
	}

	for (Condition condition : conditionList) {
	    if (!condition.verify(put)) {
		return null;
	    }
	}

	List<IncrementMessage> messages = null;
	for (Event event : eventList) {
	    IncrementMessage message = event.apply(put);
	    if (null == message) {
		continue;
	    }

	    if (null == messages) {
		messages = new LinkedList<IncrementMessage>();
	    }

	    messages.add(message);
	}

	return messages;
    }
}

