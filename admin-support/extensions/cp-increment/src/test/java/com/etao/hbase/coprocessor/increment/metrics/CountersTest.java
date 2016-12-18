/**
 * 
 */
package com.etao.hbase.coprocessor.increment.metrics;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author yutian.xb
 *
 */
public class CountersTest {
    private Counters counters;

    @Before
    public void setUp() throws Exception {
	counters = new Counters();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testIncreasePush() {
	counters.increasePush("queue", 0);
	assertEquals(0, counters.getPushItemCount("queue"));
	assertEquals(0, counters.getPushBatchCount("queue"));
	assertEquals(0, counters.getTotalPushItemCount());
	assertEquals(0, counters.getTotalPushBatchCount());

	counters.increasePush("queue", 1);
	assertEquals(1, counters.getPushItemCount("queue"));
	assertEquals(1, counters.getPushBatchCount("queue"));
	assertEquals(1, counters.getTotalPushItemCount());
	assertEquals(1, counters.getTotalPushBatchCount());

	counters.increasePush("queue", -2);
	assertEquals(-1, counters.getPushItemCount("queue"));
	assertEquals(2, counters.getPushBatchCount("queue"));
	assertEquals(-1, counters.getTotalPushItemCount());
	assertEquals(2, counters.getTotalPushBatchCount());

	counters.increasePush("queue", 3);
	assertEquals(2, counters.getPushItemCount("queue"));
	assertEquals(3, counters.getPushBatchCount("queue"));
	assertEquals(2, counters.getTotalPushItemCount());
	assertEquals(3, counters.getTotalPushBatchCount());
    }

    @Test
    public void testIncreaseDrop() {
	counters.increaseDrop("queue", 0);
	assertEquals(0, counters.getDropItemCount("queue"));
	assertEquals(0, counters.getDropBatchCount("queue"));
	assertEquals(0, counters.getTotalDropItemCount());
	assertEquals(0, counters.getTotalDropBatchCount());

	counters.increaseDrop("queue", 1);
	assertEquals(1, counters.getDropItemCount("queue"));
	assertEquals(1, counters.getDropBatchCount("queue"));
	assertEquals(1, counters.getTotalDropItemCount());
	assertEquals(1, counters.getTotalDropBatchCount());

	counters.increaseDrop("queue", -2);
	assertEquals(-1, counters.getDropItemCount("queue"));
	assertEquals(2, counters.getDropBatchCount("queue"));
	assertEquals(-1, counters.getTotalDropItemCount());
	assertEquals(2, counters.getTotalDropBatchCount());

	counters.increaseDrop("queue", 3);
	assertEquals(2, counters.getDropItemCount("queue"));
	assertEquals(3, counters.getDropBatchCount("queue"));
	assertEquals(2, counters.getTotalDropItemCount());
	assertEquals(3, counters.getTotalDropBatchCount());
    }
}

