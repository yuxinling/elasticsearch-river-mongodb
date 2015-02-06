package org.elasticsearch.river.mongodb.util;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
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
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.river.mongodb.MongoDBRiver;
import org.elasticsearch.river.mongodb.Status;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

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

    public static List<String> getRiverNames(Client client, String dbname) {
        if (StringUtils.isEmpty(dbname)) return null;
        SearchRequestBuilder request = client.prepareSearch("_river");
        request.setQuery(QueryBuilders.filteredQuery(QueryBuilders.termQuery("type", MongoDBRiver.TYPE),
                FilterBuilders.boolFilter().must(FilterBuilders.termFilter("_id", MongoDBRiver.RIVER_META))
                        .must(FilterBuilders.termFilter("dbname", dbname))));

        //request.addField("dbname");
        SearchResponse response = request.execute().actionGet();

        List<String> riverNames = Lists.newArrayList();
        for (SearchHit hit : response.getHits()) {
            if (dbname.equals(hit.getSource().get("dbname"))) {
                riverNames.add(hit.getType());
            }
        }

        return riverNames;
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

}
