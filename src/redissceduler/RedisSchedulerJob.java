package redissceduler;
import java.util.Map;

public interface RedisSchedulerJob {
	public void execute(Map<String, String> parameters);
}
