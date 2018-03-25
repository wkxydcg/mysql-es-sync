package com.wosai.mysql.es.sync.util;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by wkx on 2017/3/13.
 * @author wkx
 */
@SuppressWarnings("unchecked")
public class RedisFacade {

    private static RedisTemplate<Object, Object> redisTemplate;

    static {
        redisTemplate = (RedisTemplate) BeanFactory.getBean("redisTemplate");
        RedisSerializer stringSerializer = new StringRedisSerializer();
        Jackson2JsonRedisSerializer jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer(Object.class);
        ObjectMapper om = new ObjectMapper();
        om.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        jackson2JsonRedisSerializer.setObjectMapper(om);
        redisTemplate.setKeySerializer(stringSerializer);
        redisTemplate.setHashKeySerializer(stringSerializer);
        redisTemplate.setValueSerializer(jackson2JsonRedisSerializer);
        redisTemplate.setHashValueSerializer(jackson2JsonRedisSerializer);
    }

    public RedisFacade() {
    }

    private static RedisTemplate<Object, Object> getRedisTemplate() {
        return redisTemplate;
    }

    public static RedisTemplate<Object, Object> getRedisTemplate(Integer dbIndex) {
        return redisTemplate;
    }

    public static boolean exists(String key) {
        return getRedisTemplate().hasKey(key);
    }

    public static Object get(String key) {
        return getRedisTemplate().opsForValue().get(key);
    }

    public static List<Object> mget(Object... keys) {
        return getRedisTemplate().opsForValue().multiGet(Arrays.asList(keys));
    }

    public static void set(String key, Object value) {
        getRedisTemplate().opsForValue().set(key, value);
    }

    public static void set(String key, Object value, long time, TimeUnit unit) {
        getRedisTemplate().opsForValue().set(key, value, time, unit);
    }

    public static boolean setnx(String key, Object value) {
        return getRedisTemplate().opsForValue().setIfAbsent(key, value);
    }

    public static boolean setex(String key, Object value, int seconds) {
        boolean b = getRedisTemplate().opsForValue().setIfAbsent(key, value);
        if(b) {
            getRedisTemplate().expire(key, (long)seconds, TimeUnit.SECONDS);
        }

        return b;
    }

    public static Object getSet(String key, Object value) {
        return StringUtils.isNoneEmpty(key)?null:getRedisTemplate().opsForValue().getAndSet(key, value);
    }

    public static void append(String key, String value) {
        getRedisTemplate().opsForValue().append(key, value);
    }

    public static String substr(String key, int start, int end) {
        return getRedisTemplate().opsForValue().get(key, (long)start, (long)end);
    }

    public static boolean expire(String key, int seconds) {
        return !StringUtils.isNoneEmpty(key) && getRedisTemplate().expire(key, (long) seconds, TimeUnit.SECONDS);
    }

    public static Long ttl(String key) {
        return StringUtils.isNoneEmpty(key)?Long.valueOf(-1L):getRedisTemplate().getExpire(key);
    }

    public static void del(String... keys) {
        List c = Arrays.asList(keys);
        getRedisTemplate().delete(c);
    }

    public static boolean rename(String oldKey, String newKey) {
        return getRedisTemplate().renameIfAbsent(oldKey, newKey);
    }

    public static Long incr(String key) {
        return getRedisTemplate().opsForValue().increment(key, 1L);
    }

    public static Long incrBy(String key, long value) {
        return getRedisTemplate().opsForValue().increment(key, value);
    }

    public static Long decr(String key) {
        return getRedisTemplate().opsForValue().increment(key, -1L);
    }

    public static Long decrBy(String key, long value) {
        return getRedisTemplate().opsForValue().increment(key, value);
    }

    public static void hset(String key, Object field, Object value) {
        getRedisTemplate().opsForHash().put(key, field, value);
    }

    public static Object hget(String key, Object field) {
        return StringUtils.isNoneEmpty(key)?null:getRedisTemplate().opsForHash().get(key, field);
    }

    public static List<Object> hmget(Object key, Object... hashKeys) {
        return getRedisTemplate().opsForHash().multiGet(key, Arrays.asList(hashKeys));
    }

    public boolean hexists(Object key, Object hashKey) {
        return !isNullOrEmpty(key) && getRedisTemplate().opsForHash().hasKey(key, hashKey);
    }

    public static Long hdel(Object key, Object... hashKeys) {
        return isNullOrEmpty(key)?Long.valueOf(0L):getRedisTemplate().opsForHash().delete(key, hashKeys);
    }

    public static Long hlen(String key) {
        return StringUtils.isNoneEmpty(key)?Long.valueOf(0L):getRedisTemplate().opsForHash().size(key);
    }

    public static Set<Object> hkeys(Object key) {
        return isNullOrEmpty(key)?null:getRedisTemplate().opsForHash().keys(key);
    }

    public static List<Object> hvalues(Object key) {
        return isNullOrEmpty(key)?null:getRedisTemplate().opsForHash().values(key);
    }

    public static Map<Object, Object> hgetAll(Object key) {
        return isNullOrEmpty(key)?null:getRedisTemplate().opsForHash().entries(key);
    }

    public static Long rpush(Object key, Object... value) {
        return isNullOrEmpty(key)?null:getRedisTemplate().opsForList().rightPushAll(key, value);
    }

    public static Long lpush(Object key, Object... value) {
        return isNullOrEmpty(key)?null:getRedisTemplate().opsForList().leftPush(key, value);
    }

    public static Long llen(int database, Object key) {
        return isNullOrEmpty(key)?Long.valueOf(0L):getRedisTemplate().opsForList().size(key);
    }

    public static List<Object> lrange(Object key, long start, long end) {
        return isNullOrEmpty(key)?null:getRedisTemplate().opsForList().range(key, start, end);
    }

    public static void ltrim(Object key, long start, long end) {
        if(!isNullOrEmpty(key)) {
            getRedisTemplate().opsForList().trim(key, start, end);
        }
    }

    public static Object lindex(String key, long index) {
        return StringUtils.isNoneEmpty(key)?null:getRedisTemplate().opsForList().index(key, index);
    }

    public static void lset(Object key, long index, Object value) {
        if(!isNullOrEmpty(key)) {
            getRedisTemplate().opsForList().set(key, index, value);
        }
    }

    public static Long lrem(Object key, long count, Object value) {
        return isNullOrEmpty(key)?Long.valueOf(0L):getRedisTemplate().opsForList().remove(key, count, value);
    }

    public static Long lrem(String key, long count, String value) {
        return getRedisTemplate().opsForList().remove(key, count, value);
    }

    public static Object rpop(Object key) {
        return getRedisTemplate().opsForList().rightPop(key);
    }

    public static Object lpop(Object key) {
        return isNullOrEmpty(key)?null:getRedisTemplate().opsForList().leftPop(key);
    }

    public static Long sadd(Object key, Object... values) {
        return isNullOrEmpty(key)?null:getRedisTemplate().opsForSet().add(key, values);
    }

    public static Set<Object> smembers(Object key) {
        return isNullOrEmpty(key)?null:getRedisTemplate().opsForSet().members(key);
    }

    public static Long srem(Object key, Object... values) {
        return isNullOrEmpty(key)?null:getRedisTemplate().opsForSet().remove(key, values);
    }

    public static Object spop(Object key) {
        return isNullOrEmpty(key)?null:getRedisTemplate().opsForSet().pop(key);
    }

    public static boolean smove(Object srcKey, Object destKey, Object member) {
        return (!isNullOrEmpty(srcKey) && !isNullOrEmpty(destKey)) && ((member != null) && getRedisTemplate().opsForSet().move(srcKey, member, destKey));
    }

    public static Long slen(Object key) {
        return isNullOrEmpty(key)?null:getRedisTemplate().opsForSet().size(key);
    }

    public static boolean sismember(Object key, Object member) {
        return !isNullOrEmpty(key) && getRedisTemplate().opsForSet().isMember(key, member);
    }

    public static Set<Object> sinter(Object key, Object... keys) {
        return key == null?null:getRedisTemplate().opsForSet().intersect(key, keys);
    }

    public static Long sinterstore(Object destKey, Object srckey, Object... srcKeys) {
        return !isNullOrEmpty(destKey) && srckey != null?getRedisTemplate().opsForSet().intersectAndStore(srckey, srcKeys, destKey):null;
    }

    public static Set<Object> sunion(Object... keys) {
        if(keys != null && keys.length != 0) {
            List keyList = Arrays.asList(keys);
            return getRedisTemplate().opsForSet().union(keyList.remove(0), keyList);
        } else {
            return null;
        }
    }

    public static Long sunionstore(Object dstKey, Object... keys) {
        if(keys != null && keys.length != 0 && dstKey != null) {
            List keyList = Arrays.asList(keys);
            return getRedisTemplate().opsForSet().unionAndStore(keyList.remove(0), keyList, dstKey);
        } else {
            return null;
        }
    }

    public static Set<Object> sdiff(Object... keys) {
        if(keys != null && keys.length != 0) {
            List keyList = Arrays.asList(keys);
            return getRedisTemplate().opsForSet().difference(keyList.remove(0), keyList);
        } else {
            return null;
        }
    }

    public static Long sdiffstore(Object dstKey, Object... keys) {
        if(!isNullOrEmpty(dstKey) && keys != null && keys.length != 0) {
            List keyList = Arrays.asList(keys);
            return getRedisTemplate().opsForSet().differenceAndStore(keyList.remove(0), keyList, dstKey);
        } else {
            return null;
        }
    }

    private static boolean isNullOrEmpty(Object obj) {
        boolean result = false;
        if (obj == null || "null".equals(obj) || "".equals(obj.toString().trim())) {
            result = true;
        }
        return result;
    }
}
