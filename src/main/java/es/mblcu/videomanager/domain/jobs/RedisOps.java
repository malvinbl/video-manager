package es.mblcu.videomanager.domain.jobs;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface RedisOps {

  CompletableFuture<Map<String, String>> hgetall(String key);

  CompletableFuture<Long> hset(String key, Map<String, String> fields);

  CompletableFuture<Long> incr(String key);

  CompletableFuture<Long> decr(String key);

  CompletableFuture<Long> del(String key);

}
