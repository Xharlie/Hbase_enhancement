/**
 * 
 */
package com.etao.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author dihong.wq
 * 
 */
public class CollectionUtils {
	public static <E> BlockingQueue<E> enlargeBlockingQueue(
			BlockingQueue<E> queue, int newCapacity) {
		BlockingQueue<E> enlargedQueue = new LinkedBlockingQueue<E>(newCapacity);
		queue.drainTo(enlargedQueue);

		return enlargedQueue;
	}

	public static <E> BlockingQueue<E> shrinkBlockingQueue(
			BlockingQueue<E> queue, int newCapacity) {
		BlockingQueue<E> shrinkedQueue = new LinkedBlockingQueue<E>(newCapacity);
		queue.drainTo(shrinkedQueue, newCapacity);

		return shrinkedQueue;
	}
}
