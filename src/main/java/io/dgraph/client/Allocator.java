package io.dgraph.client;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Allocator {

    private ReentrantLock useLock = new ReentrantLock();

    private long startId;

    private long endId;

    private Map<String,Long> cache;
    
    public Allocator() {

        startId = endId = Long.valueOf(0);
        cache = Maps.newHashMap();
    }
    
    public long incrStartId() {
        return startId++;
    }

    
    public void lock() {
        useLock.lock();
    }
    
    public void unlock() {
        useLock.unlock();
    }
    
    public long cacheGet(String id) {
        Long val = cache.get(id);
        if(val == null) {
            return 0;
        }
        return val.longValue();
    }
    
    public boolean isHeldByCurrentThread() {
       return useLock.isHeldByCurrentThread();
    }
}
