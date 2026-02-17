package es.mblcu.videomanager.infrastructure.redis;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import es.mblcu.videomanager.domain.jobs.RedisOps;
import io.lettuce.core.api.StatefulRedisConnection;

public class LettuceRedisOps implements RedisOps {

  private final StatefulRedisConnection<String, String> connection;

  public LettuceRedisOps(StatefulRedisConnection<String, String> connection) {
    this.connection = connection;
  }

  @Override
  public CompletableFuture<Map<String, String>> hgetall(String key) {
    return connection.async().hgetall(key).toCompletableFuture();
  }

  @Override
  public CompletableFuture<Long> hset(String key, Map<String, String> fields) {
    return connection.async().hset(key, fields).toCompletableFuture();
  }

  @Override
  public CompletableFuture<Long> incr(String key) {
    return connection.async().incr(key).toCompletableFuture();
  }

  @Override
  public CompletableFuture<Long> decr(String key) {
    return connection.async().decr(key).toCompletableFuture();
  }

  @Override
  public CompletableFuture<Long> del(String key) {
    return connection.async().del(key).toCompletableFuture();
  }

}
