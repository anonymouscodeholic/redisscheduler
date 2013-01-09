package redissceduler;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import redis.clients.jedis.JedisPool;

public class RedisScheduler {
	/**
	 * A key for Redis, the value is a hash that has as ids values created from 
	 * <code>sequenceKey</code>. The score is the execution time. We cannot 
	 * have the data of the job directly in the set, since the set can only the same data
	 * once and we don't want to have that restriction in the data.
	 */
	private final byte[] sortedSetKey;
	
	/**
	 * A key for Redis. The value is an Oracle-like sequence for generating
	 * unique ids for the entries in the hash stored with <code>dataKey</code>
	 */
	private final byte[] sequenceKey;
	
	/**
	 * A key for Redis. The value is a hash that has as keys the unique entries
	 * from <code>sequenceKey</code> and as the value the data.
	 */
	private final byte[] dataKey;

	private final Operations operations;
	private RedisSchedulerRunnable redisSchedulerRunnable;
	private Thread thread;

	/**
	 * 
	 * @param jedisPool the connection pool for Redis instance.
	 * @param key
	 * @param sequenceKey
	 * @param dataKey
	 */
	public RedisScheduler(JedisPool jedisPool, String sortedSetKey, String sequenceKey, String dataKey) {
		this.sortedSetKey = sortedSetKey.getBytes();
		this.sequenceKey = sequenceKey.getBytes();
		this.dataKey = dataKey.getBytes();
		
		this.operations = new Operations(jedisPool, this.sortedSetKey, this.sequenceKey, this.dataKey);
	}
	
	/**
	 * Starts the polling and executing the jobs.
	 */
	public void start() {
		this.redisSchedulerRunnable = new RedisSchedulerRunnable(operations);
		this.thread = new Thread(redisSchedulerRunnable, "RedisScheduler");
		this.thread.start();
	}
	
	/**
	 * Stops the polling and executing the jobs.
	 */
	public void stop() {
		this.redisSchedulerRunnable.stop();
		this.thread.interrupt();
	}
	
	/**
	 * Schedules a <code>RedisSchedulerJob</code> to be executed at a certain point of time (at <code>timestamp</code>)
	 * in the future. When the time is reached the RedisScheduler polling mechanism will instantiate the given 
	 * class and call its <code>execute</code> method giving <code>parameters</code> as the parameters. 
	 * The parameter values are Strings to avoid any need for parameter serialization. If the user of RedisScheduler
	 * needs some other datatype, he needs to serialize the data to string.
	 * 
	 * @param jobClass the Class of the job to be executed.
	 * @param parameters
	 * @param timestamp
	 * @throws UnsupportedEncodingException
	 */
	public void schedule(Class<? extends RedisSchedulerJob> jobClass, Map<String, String> parameters, long timestamp) {
		try {
			List<String> entries = new ArrayList<String>();
			for (Entry<String, String> entry : parameters.entrySet()) {
				StringBuilder sb = new StringBuilder();
					sb.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
				sb.append("=");
				sb.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
				entries.add(sb.toString());
			}
			String paramsString = join(entries, "&");
			
			StringBuilder sb = new StringBuilder();
			sb.append("class=");
			sb.append(jobClass.getName());
			sb.append("&params=");		
			sb.append(URLEncoder.encode(paramsString, "UTF-8"));
			byte[] data = sb.toString().getBytes("UTF-8");
			
			operations.scheduleData(data, timestamp);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
	private String join(List<String> entries, String delimiter) {
	    StringBuilder sb = new StringBuilder();
	    Iterator<String> iter = entries.iterator();
	    while (iter.hasNext()) {
	        sb.append(iter.next());
	        if (iter.hasNext()) {
	        	sb.append(delimiter);
	        }
	    }
	    return sb.toString();
	}
}
