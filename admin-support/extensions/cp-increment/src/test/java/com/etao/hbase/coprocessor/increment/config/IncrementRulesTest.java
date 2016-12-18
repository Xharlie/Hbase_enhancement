/**
 * 
 */
package com.etao.hbase.coprocessor.increment.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

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
public class IncrementRulesTest {
    private JSONObject sampleJson;

    @Before
    public void setUp() throws Exception {
	sampleJson = new JSONObject();
	sampleJson.put("queue.offer.timeout", "2000");

	JSONObject queuesJson = new JSONObject();
	JSONObject attachmentRowkeyQueueJson = new JSONObject();
	attachmentRowkeyQueueJson.put("queue.size", "128");
	attachmentRowkeyQueueJson.put("writer.count", "1");
	attachmentRowkeyQueueJson.put("writer.batch.size", "20");
	attachmentRowkeyQueueJson.put("client.rpc.max.attempts", "3");
	attachmentRowkeyQueueJson.put("client.retries.num", "3");
	attachmentRowkeyQueueJson.put("rpc.timeout", "10000");
	queuesJson.put("attachment_rowkey_queue", attachmentRowkeyQueueJson);
	JSONObject b2cCrawlerRowkeyQueueJson = new JSONObject();
	b2cCrawlerRowkeyQueueJson.put("queue.size", "100");
	b2cCrawlerRowkeyQueueJson.put("writer.count", "9");
	b2cCrawlerRowkeyQueueJson.put("writer.batch.size", "10");
	b2cCrawlerRowkeyQueueJson.put("client.rpc.max.attempts", "2");
	b2cCrawlerRowkeyQueueJson.put("client.retries.num", "2");
	b2cCrawlerRowkeyQueueJson.put("rpc.timeout", "2000");
	queuesJson.put("b2c_crawler_rowkey_queue", b2cCrawlerRowkeyQueueJson);
	JSONObject linkRowkeyQueueJson = new JSONObject();
	linkRowkeyQueueJson.put("queue.size", "10");
	linkRowkeyQueueJson.put("writer.count", "1");
	linkRowkeyQueueJson.put("writer.batch.size", "5");
	linkRowkeyQueueJson.put("client.rpc.max.attempts", "1");
	linkRowkeyQueueJson.put("client.retries.num", "1");
	linkRowkeyQueueJson.put("rpc.timeout", "1000");
	queuesJson.put("link_rowkey_queue", linkRowkeyQueueJson);
	sampleJson.put("queues", queuesJson);

	JSONArray rulesArr = new JSONArray();

	JSONObject elem1Json = new JSONObject();
	elem1Json.put("trigger", "history:cp_trigger_b2c_dump");
	JSONArray events1Arr = new JSONArray();
	JSONObject events1Elem1Json = new JSONObject();
	events1Elem1Json.put("target", "b2c_crawler_rowkey_queue");
	events1Elem1Json.put("topic", "b2c_crawler");
	events1Elem1Json.put("type", "ROWKEY");
	events1Arr.put(events1Elem1Json);
	elem1Json.put("events", events1Arr);
	rulesArr.put(elem1Json);

	JSONObject elem2Json = new JSONObject();
	elem2Json.put("trigger", "history:cp_trigger_js_stock");
	JSONArray events2Arr = new JSONArray();
	JSONObject events2ArrElem1Json = new JSONObject();
	events2ArrElem1Json.put("target", "business_rowkey_queue");
	events2ArrElem1Json.put("topic", "rowkey");
	events2ArrElem1Json.put("type", "ROWKEY");
	events2Arr.put(events2ArrElem1Json);
	JSONObject events2ArrElem2Json = new JSONObject();
	events2ArrElem2Json.put("target", "attachment_rowkey_queue");
	events2ArrElem2Json.put("topic", "b2c_crawler_js_stock");
	events2ArrElem2Json.put("type", "CONTENT");
	JSONArray events2ArrElem2ContentsArr = new JSONArray();
	JSONObject events2ArrElem2ContentsArrElem1 = new JSONObject();
	events2ArrElem2ContentsArrElem1.put("source", "parser:realtitle");
	events2ArrElem2ContentsArrElem1.put("destination", "realtitle");
	events2ArrElem2ContentsArr.put(events2ArrElem2ContentsArrElem1);
	JSONObject events2ArrElem2ContentsArrElem2 = new JSONObject();
	events2ArrElem2ContentsArrElem2.put("source", "parser:now_price");
	events2ArrElem2ContentsArrElem2.put("destination", "now_price");
	events2ArrElem2ContentsArr.put(events2ArrElem2ContentsArrElem2);
	JSONObject events2ArrElem2ContentsArrElem3 = new JSONObject();
	events2ArrElem2ContentsArrElem3.put("source", "parser:image_display_url");
	events2ArrElem2ContentsArrElem3.put("destination", "image_display_url");
	events2ArrElem2ContentsArr.put(events2ArrElem2ContentsArrElem3);
	events2ArrElem2Json.put("contents", events2ArrElem2ContentsArr);
	events2Arr.put(events2ArrElem2Json);
	elem2Json.put("events", events2Arr);
	rulesArr.put(elem2Json);

	JSONObject elem3Json = new JSONObject();
	elem3Json.put("trigger", "history:cp_trigger_link");
	JSONArray events3Arr = new JSONArray();
	JSONObject events3ArrElem1Json = new JSONObject();
	events3ArrElem1Json.put("target", "link_rowkey_queue");
	events3ArrElem1Json.put("topic", "link_crawler");
	events3ArrElem1Json.put("type", "CONTENT");
	JSONArray events3ArrElem1ContentsArr = new JSONArray();
	JSONObject events3ArrElem1ContentsArrElem1 = new JSONObject();
	events3ArrElem1ContentsArrElem1.put("source", "outlinks:");
	events3ArrElem1ContentsArrElem1.put("destination", "");
	events3ArrElem1ContentsArr.put(events3ArrElem1ContentsArrElem1);
	events3ArrElem1Json.put("contents", events3ArrElem1ContentsArr);
	events3Arr.put(events3ArrElem1Json);
	elem3Json.put("events", events3Arr);
	rulesArr.put(elem3Json);

	sampleJson.put("rules", rulesArr);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testReload() throws JSONException {
	JSONObject json = null;
	IncrementRules rules = null;

	assertTrue(IncrementRules.reload(null).needReload());
	assertTrue(IncrementRules.reload("").needReload());

	json = new JSONObject(sampleJson.toString());
	rules = IncrementRules.reload(json.toString());
	assertFalse(rules.needReload());

	assertEquals(2000L, rules.getOfferTimeout());
	Map<String, Integer> queueSizeMap = rules.getQueueSizeMap();
	assertNotNull(queueSizeMap);
	assertTrue(queueSizeMap.containsKey("attachment_rowkey_queue"));
	assertEquals(128, (int) queueSizeMap.get("attachment_rowkey_queue"));
	assertTrue(queueSizeMap.containsKey("b2c_crawler_rowkey_queue"));
	assertEquals(100, (int) queueSizeMap.get("b2c_crawler_rowkey_queue"));
	assertTrue(queueSizeMap.containsKey("link_rowkey_queue"));
	assertEquals(10, (int) queueSizeMap.get("link_rowkey_queue"));

	assertEquals(1, rules.getWriterCount("attachment_rowkey_queue"));
	assertEquals(20, rules.getWriterBatchSize("attachment_rowkey_queue"));
	assertEquals(3, rules.getClientRpcMaxAttempts("attachment_rowkey_queue"));
	assertEquals(3, rules.getClientRetriesNum("attachment_rowkey_queue"));
	assertEquals(10000L, rules.getRpcTimeout("attachment_rowkey_queue"));

	assertEquals(9, rules.getWriterCount("b2c_crawler_rowkey_queue"));
	assertEquals(10, rules.getWriterBatchSize("b2c_crawler_rowkey_queue"));
	assertEquals(2, rules.getClientRpcMaxAttempts("b2c_crawler_rowkey_queue"));
	assertEquals(2, rules.getClientRetriesNum("b2c_crawler_rowkey_queue"));
	assertEquals(2000L, rules.getRpcTimeout("b2c_crawler_rowkey_queue"));

	assertEquals(1, rules.getWriterCount("link_rowkey_queue"));
	assertEquals(5, rules.getWriterBatchSize("link_rowkey_queue"));
	assertEquals(1, rules.getClientRpcMaxAttempts("link_rowkey_queue"));
	assertEquals(1, rules.getClientRetriesNum("link_rowkey_queue"));
	assertEquals(1000L, rules.getRpcTimeout("link_rowkey_queue"));

	List<Rule> ruleList = rules.getRules();
	assertNotNull(ruleList);
	assertEquals(3, ruleList.size());

	assertEquals("history:cp_trigger_b2c_dump", ruleList.get(0).getTrigger());
	assertEquals("history:cp_trigger_js_stock", ruleList.get(1).getTrigger());
	assertEquals("history:cp_trigger_link", ruleList.get(2).getTrigger());

	assertEquals(1, ruleList.get(0).getEventList().size());
	assertEquals(2, ruleList.get(1).getEventList().size());
	assertEquals(1, ruleList.get(2).getEventList().size());

	Event event = ruleList.get(0).getEventList().get(0);
	assertNotNull(event);
	assertEquals("b2c_crawler_rowkey_queue", event.getTarget());
	assertEquals("b2c_crawler", event.getTopic());
	assertEquals("ROWKEY", event.getType());

	Map<String, String> contentMap = ruleList.get(1).getEventList().get(1).getContentMap();
	assertNotNull(contentMap);
	assertEquals(3, contentMap.size());
	assertTrue(contentMap.containsKey("parser:realtitle"));
	assertTrue(contentMap.containsKey("parser:now_price"));
	assertTrue(contentMap.containsKey("parser:image_display_url"));
	assertEquals("realtitle", contentMap.get("parser:realtitle"));
	assertEquals("now_price", contentMap.get("parser:now_price"));
	assertEquals("image_display_url", contentMap.get("parser:image_display_url"));
    }

    @Test
    public void testDiff1() throws JSONException {
	JSONObject json = new JSONObject(sampleJson.toString());
	IncrementRules originalRules = IncrementRules.reload(json.toString());
	IncrementRules newRules = IncrementRules.reload(json.toString());
	IncrementRulesDiff rulesDiff = IncrementRules.diff(newRules, originalRules);
	assertNull(rulesDiff);
	assertNull(IncrementRules.diff(null, originalRules));
	assertNull(IncrementRules.diff(newRules, null));
	assertNull(IncrementRules.diff(null, null));
    }

    @Test
    public void testDiff2() throws JSONException {
	JSONObject json = new JSONObject(sampleJson.toString());
	IncrementRules originalRules = IncrementRules.reload(json.toString());

	json.remove("queue.offer.timeout");
	JSONObject subJson1 = json.getJSONObject("queues").getJSONObject("attachment_rowkey_queue");
	subJson1.remove("queue.size");
	subJson1.remove("client.rpc.max.attempts");
	subJson1.remove("client.retries.num");
	subJson1.remove("rpc.timeout");
	JSONObject subJson2 = json.getJSONObject("queues").getJSONObject("b2c_crawler_rowkey_queue");
	subJson2.remove("writer.batch.size");
	JSONObject subJson3 = json.getJSONObject("queues").getJSONObject("link_rowkey_queue");
	subJson3.remove("writer.count");
	IncrementRules newRules = IncrementRules.reload(json.toString());

	IncrementRulesDiff rulesDiff = IncrementRules.diff(newRules, originalRules);
	assertNull(rulesDiff);
    }

    @Test
    public void testDiff3() throws JSONException {
	JSONObject json = new JSONObject(sampleJson.toString());
	IncrementRules originalRules = IncrementRules.reload(json.toString());

	JSONArray rulesArr = json.getJSONArray("rules");
	JSONObject rulesArrElemJson = new JSONObject();
	rulesArrElemJson.put("trigger", "parser:image_display_url");
	rulesArrElemJson.put("events", new JSONArray());
	rulesArr.put(rulesArrElemJson);
	IncrementRules newRules = IncrementRules.reload(json.toString());

	IncrementRulesDiff rulesDiff = IncrementRules.diff(newRules, originalRules);
	assertNull(rulesDiff);
    }

    @Test
    public void testDiff4() throws JSONException {
	JSONObject json = new JSONObject(sampleJson.toString());
	IncrementRules originalRules = IncrementRules.reload(json.toString());

	JSONObject queuesJson = json.getJSONObject("queues");
	JSONObject queuesJsonElemJson = new JSONObject();
	queuesJson.put("b2c_feed_rowkey_queue", queuesJsonElemJson);
	IncrementRules newRules = IncrementRules.reload(json.toString());

	IncrementRulesDiff rulesDiff = IncrementRules.diff(newRules, originalRules);
	assertNotNull(rulesDiff);
	assertNotNull(rulesDiff.getAddList());
	assertNull(rulesDiff.getChangeList());
	assertNull(rulesDiff.getDropList());
	assertEquals(1, rulesDiff.getAddList().length);
    }

    @Test
    public void testDiff5() throws JSONException {
	JSONObject json = new JSONObject(sampleJson.toString());
	IncrementRules originalRules = IncrementRules.reload(json.toString());

	JSONObject queuesJson = json.getJSONObject("queues");
	JSONObject queuesJsonElemJson = queuesJson.getJSONObject("b2c_crawler_rowkey_queue");
	queuesJsonElemJson.put("client.rpc.max.attempts", "5");
	queuesJsonElemJson = queuesJson.getJSONObject("link_rowkey_queue");
	queuesJsonElemJson.put("queue.size", "100");
	IncrementRules newRules = IncrementRules.reload(json.toString());

	IncrementRulesDiff rulesDiff = IncrementRules.diff(newRules, originalRules);
	assertNotNull(rulesDiff);
	assertNull(rulesDiff.getAddList());
	assertNotNull(rulesDiff.getChangeList());
	assertNull(rulesDiff.getDropList());
	assertEquals(2, rulesDiff.getChangeList().length);
    }

    @Test
    public void testDiff6() throws JSONException {
	JSONObject json = new JSONObject(sampleJson.toString());
	IncrementRules originalRules = IncrementRules.reload(json.toString());

	JSONObject queuesJson = json.getJSONObject("queues");
	queuesJson.remove("attachment_rowkey_queue");
	IncrementRules newRules = IncrementRules.reload(json.toString());

	IncrementRulesDiff rulesDiff = IncrementRules.diff(newRules, originalRules);
	assertNotNull(rulesDiff);
	assertNull(rulesDiff.getAddList());
	assertNull(rulesDiff.getChangeList());
	assertNotNull(rulesDiff.getDropList());
	assertEquals(1, rulesDiff.getDropList().length);
    }

    @Test
    public void testDiff7() throws JSONException {
	JSONObject json = new JSONObject(sampleJson.toString());
	IncrementRules originalRules = IncrementRules.reload(json.toString());
	IncrementRules newRules = IncrementRules.reload(null);
	assertFalse(originalRules.needReload());
	assertTrue(newRules.needReload());

	IncrementRulesDiff rulesDiff = IncrementRules.diff(newRules, originalRules);
	assertNotNull(rulesDiff);
	assertNull(rulesDiff.getAddList());
	assertNull(rulesDiff.getChangeList());
	assertNotNull(rulesDiff.getDropList());
	assertEquals(3, rulesDiff.getDropList().length);
    }

    @Test
    public void testDiff8() throws JSONException {
	JSONObject json = new JSONObject(sampleJson.toString());
	IncrementRules originalRules = IncrementRules.reload(null);
	IncrementRules newRules = IncrementRules.reload(json.toString());
	assertTrue(originalRules.needReload());
	assertFalse(newRules.needReload());

	IncrementRulesDiff rulesDiff = IncrementRules.diff(newRules, originalRules);
	assertNotNull(rulesDiff);
	assertNotNull(rulesDiff.getAddList());
	assertNull(rulesDiff.getChangeList());
	assertNull(rulesDiff.getDropList());
	assertEquals(3, rulesDiff.getAddList().length);
    }

    @Test
    public void testDiff9() throws JSONException {
	JSONObject json = new JSONObject(sampleJson.toString());
	IncrementRules originalRules = IncrementRules.reload(json.toString());

	JSONObject queuesJson = json.getJSONObject("queues");
	JSONObject queuesJsonElemJson = queuesJson.getJSONObject("b2c_crawler_rowkey_queue");
	queuesJsonElemJson.put("writer.batch.size", "5");
	queuesJsonElemJson.put("writer.empty.interval", "500");
	queuesJsonElemJson = queuesJson.getJSONObject("link_rowkey_queue");
	queuesJsonElemJson.put("requeue.max.num", 5);
	queuesJsonElemJson.put("retry.max.ratio", "0.5");
	queuesJsonElemJson.put("writer.wait.interval", "200");
	IncrementRules newRules = IncrementRules.reload(json.toString());

	IncrementRulesDiff rulesDiff = IncrementRules.diff(newRules, originalRules);
	assertNull(rulesDiff);
    }

    @Test
    public void testApply1() throws JSONException {
	JSONObject json = new JSONObject(sampleJson.toString());
	IncrementRules rules = IncrementRules.reload(json.toString());
	assertNull(rules.apply(null));
    }

    @Test
    public void testApply2() throws JSONException {
	JSONObject json = new JSONObject(sampleJson.toString());
	IncrementRules rules = IncrementRules.reload(json.toString());

	Put put = new Put(Bytes.toBytes("http://com.sample.www/rowkey.html"));
	put.add(Bytes.toBytes("history"), Bytes.toBytes("aaa"), Bytes.toBytes("111"));
	put.add(Bytes.toBytes("parser"), Bytes.toBytes("bbb"), Bytes.toBytes("222"));

	List<IncrementMessage> messages = rules.apply(put);
	assertNull(messages);
    }

    @Test
    public void testApply3() throws JSONException {
	JSONObject json = new JSONObject(sampleJson.toString());
	IncrementRules rules = IncrementRules.reload(json.toString());

	Put put = new Put(Bytes.toBytes("http://com.sample.www/rowkey.html"));
	put.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes(""));
	put.add(Bytes.toBytes("parser"), Bytes.toBytes("bbb"), Bytes.toBytes("222"));

	List<IncrementMessage> messages = rules.apply(put);
	assertNotNull(messages);
	assertEquals(1, messages.size());

	IncrementMessage message = messages.get(0);
	assertEquals("b2c_crawler_rowkey_queue", message.getQueueName());
	assertEquals("b2c_crawler", Bytes.toString(message.getTargetMessage().getTopic()));
	assertEquals("http://com.sample.www/rowkey.html", Bytes.toString(message.getTargetMessage().getValue()));
    }

    @Test
    public void testApply4() throws JSONException {
	JSONObject json = new JSONObject(sampleJson.toString());
	IncrementRules rules = IncrementRules.reload(json.toString());

	Put put = new Put(Bytes.toBytes("http://com.sample.www/rowkey.html"));
	put.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_js_stock"), Bytes.toBytes("111"));
	put.add(Bytes.toBytes("parser"), Bytes.toBytes("realtitle"), Bytes.toBytes("222"));
	put.add(Bytes.toBytes("parser"), Bytes.toBytes("image_display_url"), Bytes.toBytes("333"));

	List<IncrementMessage> messages = rules.apply(put);
	assertNotNull(messages);
	assertEquals(2, messages.size());

	IncrementMessage message1 = messages.get(0);
	assertEquals("business_rowkey_queue", message1.getQueueName());
	assertEquals("rowkey", Bytes.toString(message1.getTargetMessage().getTopic()));
	assertEquals("http://com.sample.www/rowkey.html", Bytes.toString(message1.getTargetMessage().getValue()));

	IncrementMessage message2 = messages.get(1);
	assertEquals("attachment_rowkey_queue", message2.getQueueName());
	assertEquals("b2c_crawler_js_stock", Bytes.toString(message2.getTargetMessage().getTopic()));

	String contentJsonStr = Bytes.toString(message2.getTargetMessage().getValue());
	JSONObject contentJson = new JSONObject(contentJsonStr);
	assertTrue(contentJson.has("rowkey"));
	assertTrue(contentJson.has("content"));
	assertEquals("http://com.sample.www/rowkey.html", contentJson.getString("rowkey"));
	assertEquals("222", contentJson.getJSONObject("content").getString("realtitle"));
	assertEquals("333", contentJson.getJSONObject("content").getString("image_display_url"));
	assertFalse(contentJson.getJSONObject("content").has("now_price"));
    }

    @Test
    public void testApply5() throws JSONException {
	JSONObject json = new JSONObject(sampleJson.toString());
	IncrementRules rules = IncrementRules.reload(json.toString());

	Put put = new Put(Bytes.toBytes("http://com.sample.www/rowkey.html"));
	put.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_link"), Bytes.toBytes("111"));
	put.add(Bytes.toBytes("parser"), Bytes.toBytes("realtitle"), Bytes.toBytes("222"));
	put.add(Bytes.toBytes("outlinks"), Bytes.toBytes("aaa"), Bytes.toBytes("333"));
	put.add(Bytes.toBytes("parser"), Bytes.toBytes("image_display_url"), Bytes.toBytes("444"));
	put.add(Bytes.toBytes("outlinks"), Bytes.toBytes("bbb"), Bytes.toBytes("555"));
	put.add(Bytes.toBytes("outlinks"), Bytes.toBytes("ccc"), Bytes.toBytes(""));

	List<IncrementMessage> messages = rules.apply(put);
	assertNotNull(messages);
	assertEquals(1, messages.size());

	IncrementMessage message = messages.get(0);
	assertEquals("link_rowkey_queue", message.getQueueName());
	assertEquals("link_crawler", Bytes.toString(message.getTargetMessage().getTopic()));

	String contentJsonStr = Bytes.toString(message.getTargetMessage().getValue());
	JSONObject contentJson = new JSONObject(contentJsonStr);
	assertTrue(contentJson.has("rowkey"));
	assertTrue(contentJson.has("content"));
	assertEquals("http://com.sample.www/rowkey.html", contentJson.getString("rowkey"));
	assertEquals("333", contentJson.getJSONObject("content").getString("aaa"));
	assertEquals("555", contentJson.getJSONObject("content").getString("bbb"));
	assertEquals("", contentJson.getJSONObject("content").getString("ccc"));
    }
}

