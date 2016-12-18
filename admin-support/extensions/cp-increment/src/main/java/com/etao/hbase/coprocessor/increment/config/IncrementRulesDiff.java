/**
 * 
 */
package com.etao.hbase.coprocessor.increment.config;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * The differeces between two {@link IncrementRules}s.
 * 
 * @author yutian.xb
 * 
 */
public class IncrementRulesDiff {
    private String[] addList;
    private String[] changeList;
    private String[] dropList;

    /**
     * @return an array of added blocking queues' names
     */
    public String[] getAddList() {
	return this.addList;
    }

    /**
     * @return an array of changed blocking queues' names
     */
    public String[] getChangeList() {
	return this.changeList;
    }

    /**
     * @return an array of dropped blocking queues' names
     */
    public String[] getDropList() {
	return this.dropList;
    }

    public void setAddList(String[] addList) {
	this.addList = addList;
    }

    public void setChangeList(String[] changeList) {
	this.changeList = changeList;
    }

    public void setDropList(String[] dropList) {
	this.dropList = dropList;
    }

    public JSONObject toJson() throws JSONException {
	JSONObject json = new JSONObject();
	if (null == addList) {
	    json.put("add", JSONObject.NULL);
	} else {
	    JSONArray arr = new JSONArray();
	    for (String item : addList) {
		arr.put(item);
	    }

	    json.put("add", arr);
	}

	if (null == changeList) {
	    json.put("change", JSONObject.NULL);
	} else {
	    JSONArray arr = new JSONArray();
	    for (String item : changeList) {
		arr.put(item);
	    }

	    json.put("change", arr);
	}

	if (null == dropList) {
	    json.put("drop", JSONObject.NULL);
	} else {
	    JSONArray arr = new JSONArray();
	    for (String item : dropList) {
		arr.put(item);
	    }

	    json.put("drop", arr);
	}

	return json;
    }
}

