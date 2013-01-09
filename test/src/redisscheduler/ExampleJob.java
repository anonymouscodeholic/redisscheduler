package redisscheduler;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redissceduler.RedisSchedulerJob;

public class ExampleJob implements RedisSchedulerJob {
	private final Logger log = LoggerFactory.getLogger(ExampleJob.class);

	@Override
	public void execute(Map<String, String> parameters) {
		log.info("Job here, what's up? Value: " + parameters.get("Key"));
	}
}
