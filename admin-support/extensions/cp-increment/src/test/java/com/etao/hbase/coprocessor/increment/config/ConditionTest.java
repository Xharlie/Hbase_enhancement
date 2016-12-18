/**
 * 
 */
package com.etao.hbase.coprocessor.increment.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author yutian.xb
 *
 */
public class ConditionTest {
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testVerify1() throws JSONException {
	Condition cond1 = new Condition(null);
	assertNull(cond1.getSource());
	assertNull(cond1.getOperator());
	assertNull(cond1.getValue());

	Condition cond2 = new Condition(new JSONObject());
	assertNull(cond2.getSource());
	assertNull(cond2.getOperator());
	assertNull(cond2.getValue());
    }

    @Test
    public void testVerify2() throws JSONException {
	JSONObject cond1Json = new JSONObject();
	cond1Json.put("source", "history:cp_trigger_b2c_dump");
	cond1Json.put("operator", "STRING_NONE");
	cond1Json.put("value", "adsl");

	Condition cond1 = new Condition(cond1Json);
	assertNotNull(cond1.getSource());
	assertNull(cond1.getOperator());
	assertNotNull(cond1.getValue());

	JSONObject cond2Json = new JSONObject();
	cond2Json.put("source", "history:cp_trigger_b2c_dump");
	cond2Json.put("operator", "STRING_EQ");
	cond2Json.put("value", "adsl");

	Condition cond2 = new Condition(cond2Json);
	assertNotNull(cond2.getSource());
	assertNotNull(cond2.getOperator());
	assertNotNull(cond2.getValue());
	assertEquals("history:cp_trigger_b2c_dump", cond2.getSource());
	assertEquals("STRING_EQ", cond2.getOperator());
	assertEquals("adsl", cond2.getValue());
    }

    @Test
    public void testVerify3() throws JSONException {
	JSONObject condJson = new JSONObject();
	condJson.put("source", "history:cp_trigger_b2c_dump");
	condJson.put("operator", "STRING_EQ");
	condJson.put("value", "adsl");
	Condition cond = new Condition(condJson);

	Put put1 = new Put(Bytes.toBytes("put"));
	put1.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("adsl"));
	assertTrue(cond.verify(put1));

	Put put2 = new Put(Bytes.toBytes("put"));
	put2.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("icontent"));
	assertFalse(cond.verify(put2));
    }

    @Test
    public void testVerify4() throws JSONException {
	JSONObject condJson = new JSONObject();
	condJson.put("source", "history:cp_trigger_b2c_dump");
	condJson.put("operator", "STRING_NE");
	condJson.put("value", "adsl");
	Condition cond = new Condition(condJson);

	Put put1 = new Put(Bytes.toBytes("put"));
	put1.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("adsl"));
	assertFalse(cond.verify(put1));

	Put put2 = new Put(Bytes.toBytes("put"));
	put2.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("icontent"));
	assertTrue(cond.verify(put2));
    }

    @Test
    public void testVerify5() throws JSONException {
	JSONObject condJson = new JSONObject();
	condJson.put("source", "history:cp_trigger_b2c_dump");
	condJson.put("operator", "INT_EQ");
	condJson.put("value", "1");
	Condition cond = new Condition(condJson);

	Put put1 = new Put(Bytes.toBytes("put"));
	put1.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("1"));
	assertTrue(cond.verify(put1));

	Put put2 = new Put(Bytes.toBytes("put"));
	put2.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("2"));
	assertFalse(cond.verify(put2));

	Put put3 = new Put(Bytes.toBytes("put"));
	put3.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("abc"));
	assertFalse(cond.verify(put3));
    }

    @Test
    public void testVerify6() throws JSONException {
	JSONObject condJson = new JSONObject();
	condJson.put("source", "history:cp_trigger_b2c_dump");
	condJson.put("operator", "INT_NE");
	condJson.put("value", "1");
	Condition cond = new Condition(condJson);

	Put put1 = new Put(Bytes.toBytes("put"));
	put1.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("1"));
	assertFalse(cond.verify(put1));

	Put put2 = new Put(Bytes.toBytes("put"));
	put2.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("2"));
	assertTrue(cond.verify(put2));

	Put put3 = new Put(Bytes.toBytes("put"));
	put3.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("abc"));
	assertFalse(cond.verify(put3));
    }

    @Test
    public void testVerify7() throws JSONException {
	JSONObject condJson = new JSONObject();
	condJson.put("source", "history:cp_trigger_b2c_dump");
	condJson.put("operator", "INT_LT");
	condJson.put("value", "1");
	Condition cond = new Condition(condJson);

	Put put1 = new Put(Bytes.toBytes("put"));
	put1.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("0"));
	assertTrue(cond.verify(put1));

	Put put2 = new Put(Bytes.toBytes("put"));
	put2.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("1"));
	assertFalse(cond.verify(put2));

	Put put3 = new Put(Bytes.toBytes("put"));
	put3.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("2"));
	assertFalse(cond.verify(put3));

	Put put4 = new Put(Bytes.toBytes("put"));
	put4.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("abc"));
	assertFalse(cond.verify(put4));
    }

    @Test
    public void testVerify8() throws JSONException {
	JSONObject condJson = new JSONObject();
	condJson.put("source", "history:cp_trigger_b2c_dump");
	condJson.put("operator", "INT_LE");
	condJson.put("value", "1");
	Condition cond = new Condition(condJson);

	Put put1 = new Put(Bytes.toBytes("put"));
	put1.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("0"));
	assertTrue(cond.verify(put1));

	Put put2 = new Put(Bytes.toBytes("put"));
	put2.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("1"));
	assertTrue(cond.verify(put2));

	Put put3 = new Put(Bytes.toBytes("put"));
	put3.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("2"));
	assertFalse(cond.verify(put3));

	Put put4 = new Put(Bytes.toBytes("put"));
	put4.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("abc"));
	assertFalse(cond.verify(put4));
    }

    @Test
    public void testVerify9() throws JSONException {
	JSONObject condJson = new JSONObject();
	condJson.put("source", "history:cp_trigger_b2c_dump");
	condJson.put("operator", "INT_GT");
	condJson.put("value", "1");
	Condition cond = new Condition(condJson);

	Put put1 = new Put(Bytes.toBytes("put"));
	put1.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("0"));
	assertFalse(cond.verify(put1));

	Put put2 = new Put(Bytes.toBytes("put"));
	put2.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("1"));
	assertFalse(cond.verify(put2));

	Put put3 = new Put(Bytes.toBytes("put"));
	put3.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("2"));
	assertTrue(cond.verify(put3));

	Put put4 = new Put(Bytes.toBytes("put"));
	put4.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("abc"));
	assertFalse(cond.verify(put4));
    }

    @Test
    public void testVerify10() throws JSONException {
	JSONObject condJson = new JSONObject();
	condJson.put("source", "history:cp_trigger_b2c_dump");
	condJson.put("operator", "INT_GE");
	condJson.put("value", "1");
	Condition cond = new Condition(condJson);

	Put put1 = new Put(Bytes.toBytes("put"));
	put1.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("0"));
	assertFalse(cond.verify(put1));

	Put put2 = new Put(Bytes.toBytes("put"));
	put2.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("1"));
	assertTrue(cond.verify(put2));

	Put put3 = new Put(Bytes.toBytes("put"));
	put3.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("2"));
	assertTrue(cond.verify(put3));

	Put put4 = new Put(Bytes.toBytes("put"));
	put4.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("abc"));
	assertFalse(cond.verify(put4));
    }

    @Test
    public void testVerify11() throws JSONException {
	JSONObject condJson = new JSONObject();
	condJson.put("source", "history:cp_trigger_b2c_dump");
	condJson.put("operator", "FLOAT_EQ");
	condJson.put("value", "1");
	Condition cond = new Condition(condJson);

	Put put1 = new Put(Bytes.toBytes("put"));
	put1.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("1.0"));
	assertTrue(cond.verify(put1));

	Put put2 = new Put(Bytes.toBytes("put"));
	put2.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("2.0"));
	assertFalse(cond.verify(put2));

	Put put3 = new Put(Bytes.toBytes("put"));
	put3.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("abc"));
	assertFalse(cond.verify(put3));
    }

    @Test
    public void testVerify12() throws JSONException {
	JSONObject condJson = new JSONObject();
	condJson.put("source", "history:cp_trigger_b2c_dump");
	condJson.put("operator", "FLOAT_NE");
	condJson.put("value", "1.0");
	Condition cond = new Condition(condJson);

	Put put1 = new Put(Bytes.toBytes("put"));
	put1.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("1"));
	assertFalse(cond.verify(put1));

	Put put2 = new Put(Bytes.toBytes("put"));
	put2.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("2"));
	assertTrue(cond.verify(put2));

	Put put3 = new Put(Bytes.toBytes("put"));
	put3.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("abc"));
	assertFalse(cond.verify(put3));
    }

    @Test
    public void testVerify13() throws JSONException {
	JSONObject condJson = new JSONObject();
	condJson.put("source", "history:cp_trigger_b2c_dump");
	condJson.put("operator", "FLOAT_LT");
	condJson.put("value", "1");
	Condition cond = new Condition(condJson);

	Put put1 = new Put(Bytes.toBytes("put"));
	put1.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("0.0"));
	assertTrue(cond.verify(put1));

	Put put2 = new Put(Bytes.toBytes("put"));
	put2.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("1.0"));
	assertFalse(cond.verify(put2));

	Put put3 = new Put(Bytes.toBytes("put"));
	put3.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("2.0"));
	assertFalse(cond.verify(put3));

	Put put4 = new Put(Bytes.toBytes("put"));
	put4.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("abc"));
	assertFalse(cond.verify(put4));
    }

    @Test
    public void testVerify14() throws JSONException {
	JSONObject condJson = new JSONObject();
	condJson.put("source", "history:cp_trigger_b2c_dump");
	condJson.put("operator", "FLOAT_LE");
	condJson.put("value", "1");
	Condition cond = new Condition(condJson);

	Put put1 = new Put(Bytes.toBytes("put"));
	put1.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("0.0"));
	assertTrue(cond.verify(put1));

	Put put2 = new Put(Bytes.toBytes("put"));
	put2.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("1.0"));
	assertTrue(cond.verify(put2));

	Put put3 = new Put(Bytes.toBytes("put"));
	put3.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("2.0"));
	assertFalse(cond.verify(put3));

	Put put4 = new Put(Bytes.toBytes("put"));
	put4.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("abc"));
	assertFalse(cond.verify(put4));
    }

    @Test
    public void testVerify15() throws JSONException {
	JSONObject condJson = new JSONObject();
	condJson.put("source", "history:cp_trigger_b2c_dump");
	condJson.put("operator", "FLOAT_GT");
	condJson.put("value", "1.0");
	Condition cond = new Condition(condJson);

	Put put1 = new Put(Bytes.toBytes("put"));
	put1.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("0"));
	assertFalse(cond.verify(put1));

	Put put2 = new Put(Bytes.toBytes("put"));
	put2.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("1"));
	assertFalse(cond.verify(put2));

	Put put3 = new Put(Bytes.toBytes("put"));
	put3.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("2"));
	assertTrue(cond.verify(put3));

	Put put4 = new Put(Bytes.toBytes("put"));
	put4.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("abc"));
	assertFalse(cond.verify(put4));
    }

    @Test
    public void testVerify16() throws JSONException {
	JSONObject condJson = new JSONObject();
	condJson.put("source", "history:cp_trigger_b2c_dump");
	condJson.put("operator", "FLOAT_GE");
	condJson.put("value", "1.0");
	Condition cond = new Condition(condJson);

	Put put1 = new Put(Bytes.toBytes("put"));
	put1.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("0"));
	assertFalse(cond.verify(put1));

	Put put2 = new Put(Bytes.toBytes("put"));
	put2.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("1"));
	assertTrue(cond.verify(put2));

	Put put3 = new Put(Bytes.toBytes("put"));
	put3.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("2"));
	assertTrue(cond.verify(put3));

	Put put4 = new Put(Bytes.toBytes("put"));
	put4.add(Bytes.toBytes("history"), Bytes.toBytes("cp_trigger_b2c_dump"), Bytes.toBytes("abc"));
	assertFalse(cond.verify(put4));
    }
}

