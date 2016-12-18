/**
 * 
 */
package com.etao.hbase.coprocessor.increment.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.regex.Pattern;

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
public class EventTest {
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testApply1() throws JSONException {
	Event event1 = new Event(null);
	assertNull(event1.getTarget());
	assertNull(event1.getTopic());
	assertNull(event1.getType());
	assertNull(event1.getContentMap());

	Event event2 = new Event(new JSONObject());
	assertNull(event2.getTarget());
	assertNull(event2.getTopic());
	assertNull(event2.getType());
	assertNull(event2.getContentMap());
    }

    @Test
    public void testApply2() throws JSONException {
	JSONObject eventJson = new JSONObject();
	eventJson.put("target", "b2c_crawler_rowkey_queue");
	eventJson.put("topic", "b2c_crawler");
	eventJson.put("type", "NONE");

	Event event = new Event(eventJson);
	assertNull(event.getTarget());
	assertNull(event.getTopic());
	assertNull(event.getType());
	assertNull(event.getContentMap());
    }

    @Test
    public void testApply3() throws JSONException {
	JSONObject eventJson = new JSONObject();
	eventJson.put("target", "b2c_crawler_rowkey_queue");
	eventJson.put("topic", "b2c_crawler");
	eventJson.put("type", "ROWKEY");

	Event event = new Event(eventJson);
	assertNotNull(event.getTarget());
	assertNotNull(event.getTopic());
	assertNotNull(event.getType());
	assertNotNull(event.getContentMap());
	assertEquals(0, event.getContentMap().size());
    }

    @Test
    public void testApply4() throws JSONException {
	JSONObject eventJson = new JSONObject();
	eventJson.put("target", "b2c_crawler_rowkey_queue");
	eventJson.put("topic", "b2c_crawler");
	eventJson.put("type", "CONTENT");

	Event event = new Event(eventJson);
	assertNotNull(event.getTarget());
	assertNotNull(event.getTopic());
	assertNotNull(event.getType());
	assertNotNull(event.getContentMap());
	assertEquals(0, event.getContentMap().size());
    }

    @Test
    public void testApply5() throws JSONException {
	JSONObject eventJson = new JSONObject();
	eventJson.put("target", "b2c_crawler_rowkey_queue");
	eventJson.put("topic", "b2c_crawler");
	eventJson.put("type", "CONTENT");

	JSONArray contentsArr = new JSONArray();
	JSONObject content1Json = new JSONObject();
	content1Json.put("source", "parser:realtitle");
	content1Json.put("destination", "realtitle");
	contentsArr.put(content1Json);
	JSONObject content2Json = new JSONObject();
	content2Json.put("source", "parser:now_price");
	content2Json.put("destination", "now_price");
	contentsArr.put(content2Json);
	eventJson.put("contents", contentsArr);

	Event event = new Event(eventJson);
	assertNotNull(event.getTarget());
	assertNotNull(event.getTopic());
	assertNotNull(event.getType());
	assertNotNull(event.getContentMap());
	assertEquals(2, event.getContentMap().size());
	assertEquals("realtitle", event.getContentMap().get("parser:realtitle"));
	assertEquals("now_price", event.getContentMap().get("parser:now_price"));
    }

    @Test
    public void testApply6() throws JSONException {
	JSONObject eventJson = new JSONObject();
	eventJson.put("target", "b2c_crawler_rowkey_queue");
	eventJson.put("topic", "b2c_crawler");
	eventJson.put("type", "ROWKEY");

	Event event = new Event(eventJson);
	Put put = new Put(Bytes.toBytes("put"));
	IncrementMessage message = event.apply(put);

	assertNotNull(message);
	assertEquals("b2c_crawler_rowkey_queue", message.getQueueName());
	assertEquals("b2c_crawler", Bytes.toString(message.getTargetMessage().getTopic()));
	assertEquals("put", Bytes.toString(message.getTargetMessage().getValue()));
    }

    @Test
    public void testApply7() throws JSONException {
	JSONObject eventJson = new JSONObject();
	eventJson.put("target", "b2c_crawler_rowkey_queue");
	eventJson.put("topic", "b2c_crawler");
	eventJson.put("type", "CONTENT");

	JSONArray contentsArr = new JSONArray();
	JSONObject content1Json = new JSONObject();
	content1Json.put("source", "parser:realtitle");
	content1Json.put("destination", "realtitle");
	contentsArr.put(content1Json);
	JSONObject content2Json = new JSONObject();
	content2Json.put("source", "image:image_display_url");
	content2Json.put("destination", "image_display_url");
	contentsArr.put(content2Json);
	eventJson.put("contents", contentsArr);

	Event event = new Event(eventJson);
	Put put = new Put(Bytes.toBytes("put"));
	put.add(Bytes.toBytes("parser"), Bytes.toBytes("realtitle"), Bytes.toBytes("Fake Title"));
	put.add(Bytes.toBytes("parser"), Bytes.toBytes("special_price"), Bytes.toBytes("555.55"));
	put.add(Bytes.toBytes("image"), Bytes.toBytes("image_display_url"), Bytes.toBytes("http://www.fake.com/image.jpg"));
	IncrementMessage message = event.apply(put);

	assertNotNull(message);
	assertEquals("b2c_crawler_rowkey_queue", message.getQueueName());
	assertEquals("b2c_crawler", Bytes.toString(message.getTargetMessage().getTopic()));

	String messageJsonStr = Bytes.toString(message.getTargetMessage().getValue());
	JSONObject messageJson = new JSONObject(messageJsonStr);
	assertTrue(messageJson.has("rowkey"));
	assertTrue(messageJson.has("content"));
	assertEquals("put", messageJson.getString("rowkey"));
	assertEquals("Fake Title", messageJson.getJSONObject("content").getString("realtitle"));
	assertEquals("http://www.fake.com/image.jpg", messageJson.getJSONObject("content").getString("image_display_url"));
	assertFalse(messageJson.getJSONObject("content").has("special_price"));
    }

    @Test
    public void testApply8() throws JSONException {
	JSONObject eventJson = new JSONObject();
	eventJson.put("target", "b2c_crawler_rowkey_queue");
	eventJson.put("topic", "b2c_crawler");
	eventJson.put("type", "CONTENT");

	JSONArray contentsArr = new JSONArray();
	JSONObject content1Json = new JSONObject();
	content1Json.put("source", "outlinks:");
	content1Json.put("destination", "");
	contentsArr.put(content1Json);
	eventJson.put("contents", contentsArr);

	Event event = new Event(eventJson);
	Put put = new Put(Bytes.toBytes("put"));
	put.add(Bytes.toBytes("parser"), Bytes.toBytes("realtitle"), Bytes.toBytes("Fake Title"));
	put.add(Bytes.toBytes("outlinks"), Bytes.toBytes("http://www.fake.com/outlink_01.html"), Bytes.toBytes("outlink_params_01"));
	put.add(Bytes.toBytes("outlinks"), Bytes.toBytes("http://www.fake.com/outlink_02.html"), Bytes.toBytes("outlink_params_02"));
	put.add(Bytes.toBytes("image"), Bytes.toBytes("image_display_url"), Bytes.toBytes("http://www.fake.com/image.jpg"));
	put.add(Bytes.toBytes("outlinks"), Bytes.toBytes("http://www.fake.com/outlink_03.html"), Bytes.toBytes("outlink_params_03"));
	put.add(Bytes.toBytes("outlinks"), Bytes.toBytes("http://www.fake.com/outlink_04.html"), Bytes.toBytes("outlink_params_04"));
	IncrementMessage message = event.apply(put);

	assertNotNull(message);
	assertEquals("b2c_crawler_rowkey_queue", message.getQueueName());
	assertEquals("b2c_crawler", Bytes.toString(message.getTargetMessage().getTopic()));

	String messageJsonStr = Bytes.toString(message.getTargetMessage().getValue());
	JSONObject messageJson = new JSONObject(messageJsonStr);
	assertTrue(messageJson.has("rowkey"));
	assertTrue(messageJson.has("content"));
	assertEquals("put", messageJson.getString("rowkey"));
	assertFalse(messageJson.getJSONObject("content").has("realtitle"));
	assertEquals("outlink_params_01", messageJson.getJSONObject("content").getString("http://www.fake.com/outlink_01.html"));
	assertEquals("outlink_params_02", messageJson.getJSONObject("content").getString("http://www.fake.com/outlink_02.html"));
	assertEquals("outlink_params_03", messageJson.getJSONObject("content").getString("http://www.fake.com/outlink_03.html"));
	assertEquals("outlink_params_04", messageJson.getJSONObject("content").getString("http://www.fake.com/outlink_04.html"));
    }

    @Test
    public void testApply9() throws JSONException {
	JSONObject eventJson = new JSONObject();
	eventJson.put("target", "b2c_crawler_rowkey_queue");
	eventJson.put("topic", "b2c_crawler");
	eventJson.put("type", "CONTENT");

	Event event = new Event(eventJson);
	Put put = new Put(Bytes.toBytes("put"));
	put.add(Bytes.toBytes("parser"), Bytes.toBytes("realtitle"), Bytes.toBytes("Fake Title"));
	put.add(Bytes.toBytes("outlinks"), Bytes.toBytes("http://www.fake.com/outlink_01.html"), Bytes.toBytes("outlink_params_01"));
	put.add(Bytes.toBytes("outlinks"), Bytes.toBytes("http://www.fake.com/outlink_02.html"), Bytes.toBytes("outlink_params_02"));
	put.add(Bytes.toBytes("image"), Bytes.toBytes("image_display_url"), Bytes.toBytes("http://www.fake.com/image.jpg"));
	put.add(Bytes.toBytes("outlinks"), Bytes.toBytes("http://www.fake.com/outlink_03.html"), Bytes.toBytes("outlink_params_03"));
	put.add(Bytes.toBytes("outlinks"), Bytes.toBytes("http://www.fake.com/outlink_04.html"), Bytes.toBytes("outlink_params_04"));
	IncrementMessage message = event.apply(put);

	assertNotNull(message);
	assertEquals("b2c_crawler_rowkey_queue", message.getQueueName());
	assertEquals("b2c_crawler", Bytes.toString(message.getTargetMessage().getTopic()));

	String messageJsonStr = Bytes.toString(message.getTargetMessage().getValue());
	JSONObject messageJson = new JSONObject(messageJsonStr);
	assertTrue(messageJson.has("rowkey"));
	assertTrue(messageJson.has("content"));
	assertTrue(messageJson.has("timestamp"));
	assertEquals("put", messageJson.getString("rowkey"));
	assertTrue(Pattern.matches("\\d{13}", String.valueOf(messageJson.getLong("timestamp"))));
    }
}

