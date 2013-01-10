package redisscheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redissceduler.Operations;
import redissceduler.RedisScheduler;
import redissceduler.RedisSchedulerJob;

public class TestRedisScheduler {
	private final Logger log = LoggerFactory.getLogger(TestRedisScheduler.class);
	
	private static Map<String, String> parameters;

	private JedisPool jedisPool;

	private RedisScheduler redisScheduler;
	private Operations operations;
	
	@Before	public void setUp() throws Exception {
		jedisPool = createJedisPool();
		String sortedSetKey = "key";
		String sequenceKey = "sequenceKey";
		String dataKey = "dataKey";
		redisScheduler = new RedisScheduler(jedisPool, sortedSetKey, sequenceKey, dataKey, 1);		
		operations = new Operations(jedisPool, sortedSetKey.getBytes("UTF-8"), sequenceKey.getBytes("UTF-8"), dataKey.getBytes("UTF-8"));
		operations.flushAll();
	}
	
	@After public void tearDown() {
		operations.flushAll();
		jedisPool.destroy();
	}

	private JedisPool createJedisPool() {
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		JedisPool pool = new JedisPool(jedisPoolConfig, "localhost");
		return pool;
	}
	
	/**
	 * Tests scheduling data and polling the data. Tests this X times in a row.
	 */
	@Test public void testMultipleSequential() throws Exception {
		byte[] data = "test".getBytes();
		int count = 100;
		for (int i = 0; i < count; i++) {
			log.debug("i: " + i);
			operations.scheduleData(data, System.currentTimeMillis());
			byte[] poll = operations.pollData(System.currentTimeMillis());
			assertNotNull(poll);
			
			poll = operations.pollData(System.currentTimeMillis());
			assertNull(poll);
		}
	}

	public class MultipleParallelCallable implements Callable {
		private final Logger log = LoggerFactory.getLogger(MultipleParallelCallable.class);
		private byte[] data;
		private final CountDownLatch latch;
		private final CountDownLatch startLatch;
		private final AtomicInteger remaining;
		
		public MultipleParallelCallable(byte[] data, CountDownLatch latch, CountDownLatch startLatch, AtomicInteger remaining) {
			this.data = data;
			this.latch = latch;
			this.startLatch = startLatch;
			this.remaining = remaining;
		}
		
		@Override
		public Object call() {
			try {
				log.debug("call()");
				startLatch.await();
				
				byte[] poll = operations.pollData(System.currentTimeMillis());
				if (poll != null) {
					remaining.decrementAndGet();
				}
				return null;
			} catch (Exception e) {
//				log.error(e.getMessage(), e);
				return null;
			} finally {
				latch.countDown();
				log.debug("latch: " + latch.getCount());

			}
		}		
	}
	
	/**
	 * Tests scheduling <code>count</code> items and then polling those
	 * with <code>parallelThreads</code> parallel threads. Makes sure that 
	 * as many "jobs" are found are scheduled.
	 * 
	 * @throws Exception
	 */
	@Test public void testMultipleParallel() throws Exception {
		byte[] data = "test".getBytes();
		int count = 1000;
		int parallelThreads = 10;
		ExecutorService executorService = Executors.newFixedThreadPool(parallelThreads);
		for (int i = 0; i < count; i++) {
			operations.scheduleData(data, System.currentTimeMillis());
		}
		log.debug("Submited");
		
		
		AtomicInteger remaining = new AtomicInteger(count);
		while (remaining.get() > 0) {
			log.debug("Remaining: " + remaining.get());
			CountDownLatch startLatch = new CountDownLatch(1);
			CountDownLatch latch = new CountDownLatch(remaining.get());
			for (int i = 0; i < remaining.get(); i++) {
				executorService.submit(new MultipleParallelCallable(data, latch, startLatch, remaining));
			}
			log.debug("calling startLatch.countDown");
			startLatch.countDown();
			log.debug("await");
			latch.await();			
		}
	}
	
	public static class TestRedisSchedulerJob implements RedisSchedulerJob {
		@Override
		public void execute(Map<String, String> parameters) {
			TestRedisScheduler.parameters = parameters;
		}
	}
	
	/**
	 * Tests that a job scheduled in the past is found when polling.
	 * 
	 * @throws Exception
	 */
	@Test public void testJobScheduling() throws Exception {
		parameters = new HashMap<String, String>();
		parameters.put("test1", "value1");
		jobWithParameters(parameters, System.currentTimeMillis() - 1000);
	}

	/**
	 * Tests that when scheduling a job with parameters the same
	 * parameters are found when polling.
	 * 
	 * @throws Exception
	 */
	@Test public void testJobParams() throws Exception {
		Map<String, String> parameters = new HashMap<String, String>();
		jobWithParameters(parameters, System.currentTimeMillis());

		parameters = new HashMap<String, String>();
		parameters.put("test1", "value1");
		jobWithParameters(parameters, System.currentTimeMillis());

		parameters = new HashMap<String, String>();
		parameters.put("test1", "value1");
		parameters.put("test2", "value2");
		jobWithParameters(parameters, System.currentTimeMillis());
	}
	
	private void jobWithParameters(Map<String, String> parameters, long time) throws UnsupportedEncodingException,
			ClassNotFoundException, InstantiationException,
			IllegalAccessException {
		TestRedisScheduler.parameters = null;
		redisScheduler.schedule(TestRedisSchedulerJob.class, parameters, time);
		operations.poll(System.currentTimeMillis());
		
		assertNotNull(TestRedisScheduler.parameters);
		assertEquals(TestRedisScheduler.parameters.size(), parameters.size());
		for (Entry<String, String> entry : parameters.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			assertTrue(TestRedisScheduler.parameters.containsKey(key));
			assertEquals(value, TestRedisScheduler.parameters.get(key));
		}
	}
	
	@Test public void testBug() throws Exception {
		byte[] sortedSetKey = "z".getBytes();
		byte[] idAsBytes = "i1".getBytes();
		byte[] id2AsBytes = "i2".getBytes();

		Jedis jedis = new Jedis("localhost");
		jedis.flushAll();
		jedis.zadd(sortedSetKey, (double) 1, idAsBytes);
		jedis.zadd(sortedSetKey, (double) 2, id2AsBytes);
		jedis.disconnect();
		
		Jedis jedis2 = new Jedis("localhost");
		
		boolean testSuccessCase = false;
		if (testSuccessCase) {
			partOne(jedis, sortedSetKey, idAsBytes);
			partTwo(jedis, sortedSetKey, idAsBytes, true);
			partOne(jedis2, sortedSetKey, id2AsBytes);
			partTwo(jedis2, sortedSetKey, id2AsBytes, true);
		} else {
			partOne(jedis, sortedSetKey, idAsBytes);
			partOne(jedis2, sortedSetKey, idAsBytes);
			partTwo(jedis, sortedSetKey, idAsBytes, true);
			partTwo(jedis2, sortedSetKey, idAsBytes, false);			
		}
		
	}
	
/*
FLUSHALL
ZADD Z 0 1
ZADD Z 0 2
ZRANGE Z 0 0

***CONN1
WATCH Z
ZRANGEBYSCORE Z 0 0 WITHSCORES
MULTI

***CONN2
ZRANGEBYSCORE Z 0 0 WITHSCORES
WATCH Z
MULTI
ZREM Z 1
EXEC

***CONN1
ZREM Z 1
EXEC

 */
	
	private void partTwo(Jedis jedis, byte[] sortedSetKey, byte[] idAsBytes, boolean expectSuccess) {
		Long zremResponse;
		Transaction transaction = jedis.multi();
		Response<Long> zrem = transaction.zrem(sortedSetKey, idAsBytes);
		if (expectSuccess) {
			List<Object> allResponses = transaction.exec();
			
			zremResponse = zrem.get();
			assertNotNull(zremResponse);
		} else {
			/*
			try {
				List<Object> allResponses = transaction.exec();
				Assert.fail();
			} catch (JedisNilMultiBulkReply e) {
				
			}
			*/
		}
			
	}

	private void partOne(Jedis jedis, byte[] sortedSetKey, byte[] idAsBytes) {
		jedis.watch(sortedSetKey);
		Set<byte[]> zrange = jedis.zrange(sortedSetKey, 0, 0);
		Iterator<byte[]> iterator = zrange.iterator();
		assertTrue(iterator.hasNext());
		assertEquals(new String(idAsBytes), new String(iterator.next()));
		
		Double rank = jedis.zscore(sortedSetKey, idAsBytes);
		assertNotNull(rank);		
	}
}
