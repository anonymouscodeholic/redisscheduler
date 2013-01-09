package redissceduler;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

public class Operations {
	private final Logger log = LoggerFactory.getLogger(Operations.class);
	
	private final JedisPool jedisPool;
	private final byte[] sequenceKey;
	private final byte[] dataKey;
	private final byte[] sortedSetKey;

	public Operations(JedisPool jedisPool, byte[] sortedSetKey, byte[] sequenceKey, byte[] dataKey) {
		this.jedisPool = jedisPool;
		this.sortedSetKey = sortedSetKey;
		this.sequenceKey = sequenceKey;
		this.dataKey = dataKey;
	}
	
	public void scheduleData(byte[] data, long timestamp) {
		Jedis jedis = jedisPool.getResource();
		try {
			long id = jedis.incr(sequenceKey);
			log.debug("Created id: " + id);
			
			byte[] idAsBytes = null;
			try {
				idAsBytes = (String.valueOf(id)).getBytes("UTF-8");
			} catch (UnsupportedEncodingException e) {
				// Never thrown
			}
			// Do the operations in a pipeline for atomicity and performance
			// The id needs to be got in it's own Redis call, because
			// commands in a pipeline cannot use the results of the commands
			// in the same pipeline. Well, LUA of Redis 2.6 might do the trick....			
			Pipeline p = jedis.pipelined();
			p.hset(dataKey, idAsBytes, data);
			p.zadd(sortedSetKey, (double) timestamp, idAsBytes);
			p.syncAndReturnAll();			
		} finally {
			jedisPool.returnResource(jedis);
		}
		log.debug("Schedule data ready");
	}
	
	/**
	 * Polls a job from Redis, return its data if found a job that needs to executed, otherwise null.
	 * 
	 * @param timeAsMillis
	 * @return
	 */
	public byte[] pollData(long timeAsMillis) {
		Jedis jedis = jedisPool.getResource();

		jedis.watch(sortedSetKey);
		byte[] idAsBytes;
		try {
			Set<byte[]> zrange = jedis.zrange(sortedSetKey, 0, 0);
			Iterator<byte[]> iterator = zrange.iterator();
			if (!iterator.hasNext()) {
				log.debug("empty");
				jedis.unwatch();
				jedisPool.returnResource(jedis);
				jedis = null;
				return null;
			}
			
			idAsBytes = iterator.next();
			// TODO: should use zrange withscores instead of zrange + zscore,
			// but jedis doesn't support that!
			Double rank = jedis.zscore(sortedSetKey, idAsBytes);
			if (rank == null) {
				log.debug("Ups, no longer exists");
				jedis.unwatch();
				jedisPool.returnResource(jedis);
				jedis = null;
				return null;
			}
			if (rank > timeAsMillis) {
				log.debug("The fist job is in the future");
				jedis.unwatch();
				jedisPool.returnResource(jedis);
				jedis = null;
				return null;
			}
			long id = 0;
			try {
				id = Long.parseLong(new String(idAsBytes, "UTF-8"));
			} catch (UnsupportedEncodingException e) {
			}		
			log.debug("Found id: " + id);
			jedisPool.returnResource(jedis);
			jedis = null;
			
			jedis = jedisPool.getResource();
			Long zremResponse;
			Transaction transaction = jedis.multi();
			Response<Long> zrem = transaction.zrem(sortedSetKey, idAsBytes);
			transaction.exec();
			
			zremResponse = zrem.get();
			if (zremResponse == 0L) {
				log.debug("Not found anymore: " + id);
				jedisPool.returnResource(jedis);
				jedis = null;
				return null;
			}
			
			byte[] response = jedis.hget(dataKey, idAsBytes);
			if (response == null) {
				throw new IllegalStateException("Not found in dataKey: " + id);
			}
			Long delCount = jedis.hdel(dataKey, idAsBytes);
			if (delCount == null || delCount != 1) {
				throw new IllegalStateException("Not deleted in dataKey: " + id);
			}
			log.debug("deleted with id: " + id);
			jedisPool.returnResource(jedis);
			jedis = null;
			
			return response;
		} finally {
			if (jedis != null) {
				jedisPool.returnResource(jedis);
				jedis = null;
			}
		}
	}
	
	public boolean poll(long timeAsMillis) throws UnsupportedEncodingException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		byte[] data = this.pollData(timeAsMillis);
		if (data == null) {
			// nope, nothing found with timeAsMillis, return
			return false;
		}
		
		// Found something, parse parameters...
		String className = null;
		Map<String, String> parameters = new HashMap<String, String>();
		String dataString = new String(data, "UTF-8");
		for (String keyAndValue : dataString.split("&")) {
			String[] tokens = keyAndValue.split("=");
			String key = tokens[0];
			if (key.equals("class")) {
				String value = tokens[1];
				className = value;
			} else if (key.equals("params")) {
				if (tokens.length == 2) {
					String value = tokens[1];
					for (String paramKeyValue : URLDecoder.decode(value, "UTF-8").split("&")) {
						String[] paramTokens = paramKeyValue.split("=");
						String paramKey = paramTokens[0];
						String paramValue = paramTokens[1];
						parameters.put(URLDecoder.decode(paramKey, "UTF-8"), URLDecoder.decode(paramValue, "UTF-8"));
					}
				}
			} else {
				throw new IllegalStateException("Invalid key: " + key);
			}
		}
		// ...and execute
		@SuppressWarnings("unchecked")
		Class<? extends RedisSchedulerJob> clazz = (Class<? extends RedisSchedulerJob>) Class.forName(className);
		RedisSchedulerJob redisSchedulerJob = clazz.newInstance();
		redisSchedulerJob.execute(parameters);		
		
		return true;
	}
	
	/**
	 * Resets the RedisScheduler jobs.
	 */
	public void flushAll() {
		Jedis jedis = jedisPool.getResource();
		try {
			jedis.del(dataKey);
			jedis.del(sequenceKey);
			jedis.del(sortedSetKey);
		} finally {
			jedisPool.returnResource(jedis);
		}
	}

}
