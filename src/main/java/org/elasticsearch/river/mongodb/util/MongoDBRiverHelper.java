package org.elasticsearch.river.mongodb.util;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.mongodb.ServerAddress;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.mongodb.MongoDBRiver;
import org.elasticsearch.river.mongodb.MongoDBRiverDefinition;
import org.elasticsearch.river.mongodb.Status;
import org.elasticsearch.river.mongodb.Timestamp;
import org.elasticsearch.search.sort.SortOrder;

public abstract class MongoDBRiverHelper {

    private static final ESLogger logger = Loggers.getLogger(MongoDBRiverHelper.class);

    public static Status getRiverStatus(Client client, String riverName) {
        GetResponse statusResponse = client.prepareGet("_river", riverName, MongoDBRiver.STATUS_ID).get();
        if (!statusResponse.isExists()) {
            return Status.UNKNOWN;
        } else {
            Object obj = XContentMapValues.extractValue(MongoDBRiver.TYPE + "." + MongoDBRiver.STATUS_FIELD,
                    statusResponse.getSourceAsMap());
            return Status.valueOf(obj.toString());
        }
    }

    public static void setRiverStatus(Client client, String riverName, Status status) {
        logger.info("setRiverStatus called with {} - {}", riverName, status);
        XContentBuilder xb;
        try {
            xb = jsonBuilder().startObject().startObject(MongoDBRiver.TYPE).field(MongoDBRiver.STATUS_FIELD, status).endObject()
                    .endObject();
            client.prepareIndex("_river", riverName, MongoDBRiver.STATUS_ID).setSource(xb).get();
        } catch (IOException ioEx) {
            logger.error("setRiverStatus failed for river {}", ioEx, riverName);
        }
    }

    public static Status getMongoStatus(Client client, String riverName) {
        GetResponse statusResponse = client.prepareGet("_river", riverName, MongoDBRiver.MONGO_STATUS).get();
        if (!statusResponse.isExists()) {
            return Status.MONGO_NORMAL;
        } else {
            Object obj = XContentMapValues.extractValue(MongoDBRiver.TYPE + "." + MongoDBRiver.STATUS_MONGO,
                    statusResponse.getSourceAsMap());
            return Status.valueOf(obj.toString());
        }
    }

    public static void setMongoStatus(Client client, String riverName, Status status) {
        logger.info("setMongoStatus called with {} - {}", riverName, status);
        XContentBuilder xb;
        try {
            xb = jsonBuilder().startObject().startObject(MongoDBRiver.TYPE).field(MongoDBRiver.STATUS_MONGO, status).endObject()
                    .endObject();
            client.prepareIndex("_river", riverName, MongoDBRiver.MONGO_STATUS).setSource(xb).get();
        } catch (IOException ioEx) {
            logger.error("setMongoStatus failed for river {}", ioEx, riverName);
        }
    }

    public static Map<String, Object> getSetting(Client client, String riverName) {
        GetResponse statusResponse = client.prepareGet("_river", riverName, MongoDBRiver.RIVER_META).get();
        if (!statusResponse.isExists()) {
            return null;
        } else {
            return statusResponse.getSourceAsMap();
        }
    }

    public static void mongoUpdateRecord(Client client, String riverName, List<Map<String, Object>> from, List<Map<String, Object>> to) {

        logger.info("saveRiverUpdate record {} - {} - {}", riverName, Arrays.toString(from.toArray()), Arrays.toString(to.toArray()));
        XContentBuilder xb;
        try {

            xb = jsonBuilder()
                    .startObject()
                    .field("riverName", riverName)
                    .field("from", from)
                    .field("to", to)
                    .field("timestamp", System.currentTimeMillis())
                    .endObject();
            client.prepareIndex("_river", "river_update").setSource(xb).get();
        } catch (IOException ioEx) {
            logger.error("saveRiverUpdate failed for river {}", ioEx, riverName);
        }
    }

    public static void deleteLastTime(Client client, MongoDBRiverDefinition definition) {
        client.prepareDelete("_river", definition.getRiverName(), definition.getMongoOplogNamespace()).get();
    }

    public static void updateMongo(Client client, String riverName, List<Map<String, Object>> servers) {
        try {
            Map<String, Object> setting = getSetting(client, riverName);
            if (setting != null && setting.containsKey(MongoDBRiver.TYPE)) {

                Map<String, Object> mongo = (Map<String, Object>) setting.get(MongoDBRiver.TYPE);
                if (mongo != null) {

                    List<Map<String, Object>> source = (List<Map<String, Object>>) mongo.get("servers");

                    if (validateUpdate(servers, source)) {
                        mongo.put("servers", servers);

                        client.prepareIndex("_river", riverName, MongoDBRiver.RIVER_META).setSource(setting).get();
                        mongoUpdateRecord(client, riverName, source, servers);
                        setMongoStatus(client, riverName, Status.MONGO_UPDATE);
                    }
                }
            }
        } catch (Exception ioEx) {
            logger.error("updateMongServers failed for river {}", ioEx, riverName);
        }
    }

    public static boolean isRiverMetaExist(Client client) {
        return client.admin().indices().exists(new IndicesExistsRequest("_river")).actionGet().isExists();
    }

    public static boolean isRiverExist(Client client, String riverName) {
        if (isRiverMetaExist(client)) {
            return client.admin().indices().typesExists(new TypesExistsRequest(new String[]{"_river"}, riverName)).actionGet().isExists();
        }
        return false;
    }

    /**
     * [{"host":"10.10.0.5","port":27017},{"host":"10.10.0.6","port":27017},{"host":"10.10.10.121","port":27017}]
     *
     * @param servers
     * @param sourceServers
     * @return
     */
    public static boolean validateUpdate(List<Map<String, Object>> servers, List<Map<String, Object>> sourceServers) throws UnknownHostException {
        List<ServerAddress> addresses = addressBuilder(servers);
        if (sourceServers != null) {
            List<ServerAddress> sourceAddress = addressBuilder(sourceServers);
            if (sourceAddress.size() == addresses.size()
                    && sourceAddress.containsAll(addresses)) {
                return false;
            }

        } else {
            if (addresses.size() == 1
                    && addresses.get(0).getHost().equals("127.0.0.1")) {
                return false;
            }
        }
        return true;
    }

    public static List<ServerAddress> addressBuilder(List<Map<String, Object>> sourceServers) throws UnknownHostException {
        List<ServerAddress> addresses = Lists.newArrayList();

        for (Map<String, Object> server : sourceServers) {
            String host = (String) server.get("host");
            if (host.equals("localhost")) host = "127.0.0.1";
            int port = (int) server.get("port");
            ServerAddress address = new ServerAddress(host, port);

            addresses.add(address);
        }
        return addresses;
    }

}
