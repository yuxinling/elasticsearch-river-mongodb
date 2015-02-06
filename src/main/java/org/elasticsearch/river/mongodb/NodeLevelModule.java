package org.elasticsearch.river.mongodb;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.mongodb.ds.DataSource;

public class NodeLevelModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(MongoClientService.class).asEagerSingleton();
        bind(DataSource.class).asEagerSingleton();
    }
}
