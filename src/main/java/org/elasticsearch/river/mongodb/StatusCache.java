package org.elasticsearch.river.mongodb;

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * User: yuyangning
 * Date: 2/6/15
 * Time: 11:43 AM
 */
public class StatusCache {

    private Map<String, Status> status = Maps.newConcurrentMap();
    private static StatusCache cache = null;

    private StatusCache() {
    }

    public static StatusCache instance() {
        if (cache == null) {
            synchronized (StatusCache.class) {
                if (cache == null) {
                    cache = new StatusCache();
                }
            }
        }
        return cache;
    }

    public Status getStatus(String riverName) {
        if (StringUtils.isEmpty(riverName)) return null;
        return this.status.get(riverName);
    }

    public void setStatus(String riverName, Status status) {
        this.status.put(riverName, status);
    }
}
