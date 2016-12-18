/**
 * 
 */
package com.etao.hbase.coprocessor.increment.config;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.etao.hadoop.hbase.queue.client.Message;
import com.etao.hbase.coprocessor.increment.IncrementMessage;

/**
 * @author yutian.xb
 *
 */
public class Event {
    private static final Log LOG = LogFactory.getLog(Event.class);

    public enum Type {
	ROWKEY, CONTENT
    }

    private String target;
    private String topic;
    private Type type;
    private Map<String, String> contentMap;

    public String getTarget() {
	return this.target;
    }

    public String getTopic() {
	return this.topic;
    }

    public String getType() {
	if (null != type) {
	    return type.toString();
	}

	return null;
    }

    public Map<String, String> getContentMap() {
	return this.contentMap;
    }

    public Event(JSONObject json) throws JSONException {
	if (null == json) {
	    return;
	}

	if (!json.has("target") || !json.has("topic") || !json.has("type")) {
	    return;
	}

	try {
	    type = Type.valueOf(json.getString("type").trim());
	} catch (IllegalArgumentException e) {
	    LOG.warn(e.toString());
	    return;
	}

	if (!Type.ROWKEY.equals(type) && !Type.CONTENT.equals(type)) {
	    return;
	}

	target = json.getString("target");
	topic = json.getString("topic");
	contentMap = new TreeMap<String, String>();
	if (Type.CONTENT.equals(type) && json.has("contents")) {
	    JSONArray contentsArr = json.getJSONArray("contents");
	    for (int i = 0; i < contentsArr.length(); ++i) {
		JSONObject contentJson = contentsArr.getJSONObject(i);
		if (contentJson.has("source") && contentJson.has("destination")) {
		    contentMap.put(contentJson.getString("source"), contentJson.getString("destination"));
		}
	    }
	}
    }

    public IncrementMessage apply(Put put) {
	if (null == target || null == topic || null == type) {
	    return null;
	}

	if (Type.CONTENT.equals(type)) {
	    // rowkey, contents and timestamp in json format
	    JSONObject result = new JSONObject();
	    JSONObject contentJson = new JSONObject();
	    try {
		for (Entry<String, String> entry : contentMap.entrySet()) {
		    String source = entry.getKey();
		    if (source.endsWith(":")) {
			// filter by family
			byte[] familyBytes = Bytes.toBytes(source.substring(0, source.length() - 1));
			Map<byte[], List<KeyValue>> familyMap = put.getFamilyMap();
			if (!familyMap.containsKey(familyBytes)) {
			    continue;
			}

			List<KeyValue> kvList = familyMap.get(familyBytes);
			if (null == kvList || kvList.isEmpty()) {
			    continue;
			}

			Map<String, Long> maxTsMap = new TreeMap<String, Long>();
			for (KeyValue kv : kvList) {
			    String qualifier = Bytes.toString(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
			    ;
			    if (!maxTsMap.containsKey(qualifier)) {
				maxTsMap.put(qualifier, -1L);
			    }

			    long maxTs = maxTsMap.get(qualifier);
			    long ts = Bytes.toLong(kv.getBuffer(), kv.getTimestampOffset());
			    if (ts >= maxTs) {
				maxTsMap.put(qualifier, ts);
				contentJson.put(qualifier, Bytes.toString(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength()));
			    }
			}
		    } else {
			// filter by family & qualifier
			byte[][] sourceBytes = KeyValue.parseColumn(Bytes.toBytes(source));
			if (!put.has(sourceBytes[0], sourceBytes[1])) {
			    continue;
			}

			Cell topCell = put.get(sourceBytes[0], sourceBytes[1]).get(0);
			contentJson.put(entry.getValue(), Bytes.toString(topCell.getValueArray(), topCell.getValueOffset(), topCell.getValueLength()));
		    }
		}

		result.put("rowkey", Bytes.toString(put.getRow()));
		result.put("content", contentJson);
		result.put("timestamp", EnvironmentEdgeManager.currentTime());
	    } catch (JSONException e) {
		return null;
	    }

	    // rowkey in plain text
	    return new IncrementMessage(target, new Message(Bytes.toBytes(topic), Bytes.toBytes(result.toString())));
	}

	// only rowkey in plain text format
	return new IncrementMessage(target, new Message(Bytes.toBytes(topic), put.getRow()));
    }
}

