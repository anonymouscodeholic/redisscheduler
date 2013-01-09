package redissceduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisSchedulerRunnable implements Runnable {
	private final Logger log = LoggerFactory.getLogger(RedisSchedulerRunnable.class);
	
	private final Operations operations;
	private volatile boolean running;

	public RedisSchedulerRunnable(Operations operations) {
		this.operations = operations;
		
		this.running = true;
	}

	@Override
	public void run() {
		while (running) {
			try {
				// Process until nothing left
				while (this.operations.poll(System.currentTimeMillis())) {					
				}
			} catch (Exception e) {
				log.error("Job execution fail: " + e.getMessage(), e);
			}
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
	}

	public void stop() {
		this.running = false;
	}
}
