/**
 * 
 */
package com.etao.hbase.coprocessor.increment.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.etao.hbase.coprocessor.increment.IncrementMessage;

/**
 * @author yutian.xb
 *
 */
public class RuleTest {
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testApply1() throws JSONException {
	Rule rule1 = new Rule(null);
	assertNull(rule1.getTrigger());
	assertNotNull(rule1.getConditionList());
	assertEquals(0, rule1.getConditionList().size());
	assertNotNull(rule1.getEventList());
	assertEquals(0, rule1.getEventList().size());

	Rule rule2 = new Rule(new JSONObject());
	assertNull(rule2.getTrigger());
	assertNotNull(rule2.getConditionList());
	assertEquals(0, rule2.getConditionList().size());
	assertNotNull(rule2.getEventList());
	assertEquals(0, rule2.getEventList().size());
    }

    @Test
    public void testApply2() throws JSONException {
	JSONObject ruleJson = new JSONObject();
	ruleJson.put("trigger", "history:cp_trigger_b2c_dump");

	JSONArray conditionsArr = new JSONArray();
	JSONObject cond1 = new JSONObject();
	cond1.put("source", "history:cp_trigger_b2c_dump");
	cond1.put("operator", "STRING_EQ");
	cond1.put("value", "adsl");
	conditionsArr.put(cond1);
	ruleJson.put("conditions", conditionsArr);

	JSONArray eventsArr = new JSONArray();
	JSONObject event1Json = new JSONObject();
	event1Json.put("target", "b2c_crawler_rowkey_queue");
	event1Json.put("topic", "b2c_crawler");
	event1Json.put("type", "ROWKEY");
	eventsArr.put(event1Json);
	ruleJson.put("events", eventsArr);

	Rule rule = new Rule(ruleJson);
	assertNotNull(rule.getTrigger());
	assertEquals("history:cp_trigger_b2c_dump", rule.getTrigger());
	assertNotNull(rule.getConditionList());
	assertEquals(1, rule.getConditionList().size());
	assertNotNull(rule.getEventList());
	assertEquals(1, rule.getEventList().size());

	assertEquals("history:cp_trigger_b2c_dump", rule.getConditionList().get(0).getSource());
	assertEquals("STRING_EQ", rule.getConditionList().get(0).getOperator());
	assertEquals("adsl", rule.getConditionList().get(0).getValue());

	assertEquals("b2c_crawler_rowkey_queue", rule.getEventList().get(0).getTarget());
	assertEquals("b2c_crawler", rule.getEventList().get(0).getTopic());
	assertEquals("ROWKEY", rule.getEventList().get(0).getType());
	assertNotNull(rule.getEventList().get(0).getContentMap());
	assertEquals(0, rule.getEventList().get(0).getContentMap().size());
    }

    @Test
    public void testApply3() throws JSONException {
	JSONObject ruleJson = new JSONObject();
	ruleJson.put("trigger", "history:cp_trigger_b2c_dump");

	JSONArray conditionsArr = new JSONArray();
	JSONObject cond1 = new JSONObject();
	cond1.put("source", "history:cp_trigger_b2c_dump");
	cond1.put("operator", "STRING_EQ");
	cond1.put("value", "adsl");
	conditionsArr.put(cond1);
	JSONObject cond2 = new JSONObject();
	cond2.put("source", "parser:now_price");
	cond2.put("operator", "FLOAT_GE");
	cond2.put("value", "10.0");
	conditionsArr.put(cond2);
	ruleJson.put("conditions", conditionsArr);

	JSONArray eventsArr = new JSONArray();
	JSONObject event1Json = new JSONObject();
	event1Json.put("target", "b2c_crawler_rowkey_queue");
	event1Json.put("topic", "b2c_crawler");
	event1Json.put("type", "ROWKEY");
	eventsArr.put(event1Json);
	JSONObject event2Json = new JSONObject();
	event2Json.put("target", "b2c_feed_rowkey_queue");
	event2Json.put("topic", "b2c_feed");
	event2Json.put("type", "ROWKEY");
	eventsArr.put(event2Json);
	ruleJson.put("events", eventsArr);

	Rule rule = new Rule(ruleJson);
	Put put1 = new Put(Bytes.toBytes("put"));
	put1.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("adsl"));
	put1.add(Bytes.toBytes("parser"), Bytes.toBytes("now_price"), Bytes.toBytes("10"));
	List<IncrementMessage> messages1 = rule.apply(put1);
	assertNotNull(messages1);
	assertEquals(2, messages1.size());

	Put put2 = new Put(Bytes.toBytes("put"));
	put2.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("adsl"));
	put2.add(Bytes.toBytes("parser"), Bytes.toBytes("now_price"), Bytes.toBytes("9.9"));
	List<IncrementMessage> messages2 = rule.apply(put2);
	assertNull(messages2);
    }
}

