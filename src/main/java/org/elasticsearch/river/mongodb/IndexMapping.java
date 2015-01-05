package org.elasticsearch.river.mongodb;

import java.util.Map;

/**
 * User: yuyangning
 * Date: 1/5/15
 * Time: 2:37 PM
 */
public class IndexMapping {

    private Map<String, Object> mapping;
    private String parentId;

    public Map<String, Object> getMapping() {
        return mapping;
    }

    public void setMapping(Map<String, Object> mapping) {
        this.mapping = mapping;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }
}
