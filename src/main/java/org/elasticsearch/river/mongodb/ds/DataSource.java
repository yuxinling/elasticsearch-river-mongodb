package org.elasticsearch.river.mongodb.ds;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.zcloud.sun.datasource.SunConfig;
import com.zcloud.sun.datasource.SunConfigFactory;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.mongodb.Status;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;

import java.util.List;
import java.util.Map;

/**
 * User: yuyangning
 * Date: 2/5/15
 * Time: 4:27 PM
 */
@Singleton
public class DataSource {
    private static final ESLogger logger = ESLoggerFactory.getLogger(DataSource.class.getName());
    private SunConfigFactory sunConfigFactory = SunConfigFactory.instance();
    private Client client;

    @Inject
    public DataSource(Client esClient) {
        this.client = esClient;
        sunConfigFactory.addSunConfigChangedListener(new SunConfigFactory.SunConfigChangedListener() {
            @Override
            public void process(String schema, SunConfig before, SunConfig after) {

                if (schema == null || after == null || after.mongoAdds == null) return;
                if (before != null && before.mongoAdds != null
                        && before.mongoAdds.equals(after.mongoAdds)) return;

                logger.info("Receive the SunConfigChange event schema:{} after:{}.", schema, after.mongoAdds);

                List<String> riverNames = MongoDBRiverHelper.getRiverNames(client, schema);
                if (riverNames != null) {
                    for (String riverName : riverNames) {
                        Status status = MongoDBRiverHelper.getRiverStatus(client, riverName);
                        if (status == Status.RUNNING) {
                            MongoDBRiverHelper.setRiverStatus(client, riverName, Status.RESTART);
                        }
                    }
                }
            }
        });
    }

    public List<Map<String, Object>> getDatabaseServers(String dbname) {

        if (StringUtils.isEmpty(dbname)) return null;
        SunConfig schema = sunConfigFactory.getSunConfig(dbname);
        List<Server> servers = ServerParser.parse(schema.mongoAdds);

        List<Map<String, Object>> hosts = Lists.newArrayList();
        for (Server server : servers) {

            Map<String, Object> host = Maps.newHashMap();
            host.put("host", server.getHost());
            host.put("port", server.getPort());

            hosts.add(host);
        }

        return hosts;
    }

}
