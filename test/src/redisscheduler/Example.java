package redisscheduler;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redissceduler.RedisScheduler;

public class Example {
	private final Logger log = LoggerFactory.getLogger(Example.class);
	
	private final String[] args;

	public Example(String[] args) {
		this.args = args;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Example(args).run();
	}

	private void run() {
		// Create connection pool
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		JedisPool jedisPool = new JedisPool(jedisPoolConfig, "localhost");

		// RedisScheduler uses three keys in Redis.
		// The keys need to be such that they don't overlap with other keys in your Redis server
		RedisScheduler redisScheduler = new RedisScheduler(jedisPool, "RS_SSK", "RS_SK", "RS_DK", 10); 

		// Start polling
		redisScheduler.start();
		
		log.info("Let's schedule something to happen in five secs in the future");
		Map<String, String> parameters = new HashMap<String, String>();
		parameters.put("Key", "Value");
		redisScheduler.schedule(ExampleJob.class, parameters, System.currentTimeMillis() + 5000);
		try {
			Thread.sleep(6000);
		} catch (InterruptedException e) {
		}
		log.info("Main thread waited six seconds");
		
		// Stop polling
		redisScheduler.shutdown();
		
		// Shutdown doesn't shutdown immediately if there are pending jobs
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
		}
		
		redisScheduler.shutdownNow();		
	}
}
