package org.elasticsearch.river.mongodb;

import java.util.Map;

/**
 * User: yuyangning
 * Date: 1/5/15
 * Time: 2:37 PM
 */
public class IndexConfig {

    private Map<String, Object> mapping;
    private Map<String, Object> indexSetting;

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

    public Map<String, Object> getIndexSetting() {
        return indexSetting;
    }

    public void setIndexSetting(Map<String, Object> indexSetting) {
        this.indexSetting = indexSetting;
    }
}
