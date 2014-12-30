package org.elasticsearch.rest.action.mongodb;

import com.google.common.collect.Maps;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.*;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.mongodb.MongoDBRiver;
import org.elasticsearch.river.mongodb.MongoDBRiverDefinition;
import org.elasticsearch.river.mongodb.Status;
import org.elasticsearch.river.mongodb.Timestamp;
import org.elasticsearch.river.mongodb.rest.XContentThrowableRestResponse;
import org.elasticsearch.river.mongodb.rest.action.support.RestXContentBuilder;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RestMongoDBRiverAction extends BaseRestHandler {

    private final String riverIndexName;

    @Inject
    public RestMongoDBRiverAction(Settings settings, Client esClient, RestController controller, @RiverIndexName String riverIndexName) {
        super(settings, esClient);
        this.riverIndexName = riverIndexName;
        //String baseUrl = "/" + riverIndexName + "/" + MongoDBRiver.TYPE;
        String baseUrl = "/" + riverIndexName;
        logger.trace("RestMongoDBRiverAction - baseUrl: {}", baseUrl);
        controller.registerHandler(RestRequest.Method.GET, baseUrl + "/{river}/get", this);
        controller.registerHandler(RestRequest.Method.GET, baseUrl + "/{river}/start", this);
        controller.registerHandler(RestRequest.Method.GET, baseUrl + "/{river}/stop", this);
        controller.registerHandler(RestRequest.Method.GET, baseUrl + "/{river}/list", this);
        controller.registerHandler(RestRequest.Method.GET, baseUrl + "/{river}/delete", this);
        controller.registerHandler(RestRequest.Method.PUT, baseUrl + "/{river}/update", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client esClient) throws Exception {
        logger.debug("uri: {}", request.uri());
        logger.debug("action: {}", request.param("action"));

        if (request.path().endsWith("list")) {
            list(request, channel, esClient);
            return;
        } else if (request.path().endsWith("get")) {
            get(request, channel, esClient);
            return;
        } else if (request.path().endsWith("update")) {
            update(request, channel, esClient);
            return;
        } else if (request.path().endsWith("start")) {
            start(request, channel, esClient);
            return;
        } else if (request.path().endsWith("stop")) {
            stop(request, channel, esClient);
            return;
        } else if (request.path().endsWith("delete")) {
            delete(request, channel, esClient);
            return;
        }

        respondError(request, channel, "action not found: " + request.uri(), RestStatus.OK);
    }

    private void delete(RestRequest request, RestChannel channel, Client esClient) {
        String river = request.param("river");
        if (river == null || river.isEmpty()) {
            respondError(request, channel, "Parameter 'river' is required", RestStatus.BAD_REQUEST);
            return;
        }
        logger.info("Delete river: {}", river);
        if (MongoDBRiverHelper.isRiverExist(esClient, river)) {
            esClient.admin().indices().prepareDeleteMapping(riverIndexName).setType(river).get();
            respondSuccess(request, channel, RestStatus.OK);
            return;
        }

        respondError(request, channel, "Does not exist river with '" + river + "'", RestStatus.BAD_REQUEST);
    }

    private void start(RestRequest request, RestChannel channel, Client esClient) {
        String river = request.param("river");
        if (river == null || river.isEmpty()) {
            respondError(request, channel, "Parameter 'river' is required", RestStatus.BAD_REQUEST);
            return;
        }
        if (MongoDBRiverHelper.isRiverExist(esClient, river)) {
            MongoDBRiverHelper.setRiverStatus(esClient, river, Status.RUNNING);
            respondSuccess(request, channel, RestStatus.OK);
            return;
        }
        respondError(request, channel, "Does not exist river with '" + river + "'", RestStatus.BAD_REQUEST);
    }

    private void stop(RestRequest request, RestChannel channel, Client esClient) {
        String river = request.param("river");
        if (river == null || river.isEmpty()) {
            respondError(request, channel, "Parameter 'river' is required", RestStatus.BAD_REQUEST);
            return;
        }
        if (MongoDBRiverHelper.isRiverExist(esClient, river)) {
            MongoDBRiverHelper.setRiverStatus(esClient, river, Status.STOPPED);
            respondSuccess(request, channel, RestStatus.OK);
            return;
        }
        respondError(request, channel, "Does not exist river with '" + river + "'", RestStatus.BAD_REQUEST);
    }

    private void list(RestRequest request, RestChannel channel, Client esClient) {
        try {
            Map<String, Object> rivers = getRivers(request.paramAsInt("page", 1), request.paramAsInt("count", 10), esClient);
            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
            builder.value(rivers);
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        } catch (Throwable e) {
            errorResponse(request, channel, e);
        }
    }

    private void get(RestRequest request, RestChannel channel, Client esClient) {
        String riverName = request.param("river");
        if (riverName == null || riverName.isEmpty()) {
            respondError(request, channel, "Parameter 'river' is required", RestStatus.BAD_REQUEST);
            return;
        }
        try {
            Map<String, Object> river = getRiver(request.param("river"), esClient);
            if (river != null) {
                XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                builder.startObject();
                builder.field("_source", river);
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } else {
                respondError(request, channel, "Does not found the river[" + riverName + "]", RestStatus.NOT_FOUND);
            }
        } catch (Throwable e) {
            errorResponse(request, channel, e);
        }
    }

    private void update(RestRequest request, RestChannel channel, Client esClient) {
        try {
            String river = request.param("river");
            if (river == null || river.isEmpty()) {
                respondError(request, channel, "Parameter 'river' is required", RestStatus.BAD_REQUEST);
                return;
            }

            if (!MongoDBRiverHelper.isRiverExist(esClient, river)) {
                respondError(request, channel, "Does not exist river with '" + river + "'", RestStatus.BAD_REQUEST);
                return;
            }

            if (request.hasContent()) {
                Map<String, Object> servers = SourceLookup.sourceAsMap(request.content());
                if (servers == null || !servers.containsKey("servers")) {
                    respondError(request, channel, "Parameter 'servers' is required", RestStatus.BAD_REQUEST);
                }
                List<Map<String, Object>> address = (List<Map<String, Object>>) servers.get("servers");
                MongoDBRiverHelper.updateMongo(esClient, river, address);
            }
            respondSuccess(request, channel, RestStatus.OK);
        } catch (Exception e) {
            e.printStackTrace();
            respondError(request, channel, "Update 'servers' error:" + e.getMessage(), RestStatus.BAD_REQUEST);
        }

    }

    private void respondSuccess(RestRequest request, RestChannel channel, RestStatus status) {
        try {
            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
            builder.startObject();
            builder.field("success", true);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(status, builder));
        } catch (IOException e) {
            errorResponse(request, channel, e);
        }
    }

    private void respondError(RestRequest request, RestChannel channel, String error, RestStatus status) {
        try {
            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
            builder.startObject();
            builder.field("success", false);
            builder.field("error", error);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(status, builder));
        } catch (IOException e) {
            errorResponse(request, channel, e);
        }
    }

    private void errorResponse(RestRequest request, RestChannel channel, Throwable e) {
        try {
            channel.sendResponse(new XContentThrowableRestResponse(request, e));
            logger.error("errorResponse", e);
        } catch (IOException ioEx) {
            logger.error("Failed to send failure response", ioEx);
        }
    }

    private Map<String, Object> getRivers(int page, int count, Client esClient) {

        Map<String, Object> data = Maps.newHashMap();
        if (!MongoDBRiverHelper.isRiverMetaExist(esClient)) return data;

        int from = (page - 1) * count;
        SearchResponse searchResponse = esClient.prepareSearch(riverIndexName)
                .setQuery(QueryBuilders.queryString(MongoDBRiver.TYPE).defaultField("type")).setFrom(from).setSize(count).get();
        long totalHits = searchResponse.getHits().totalHits();
        logger.trace("totalHits: {}", totalHits);
        data.put("hits", totalHits);
        data.put("page", page);
        data.put("pages", Math.ceil(totalHits / (float) count));
        List<Map<String, Object>> rivers = new ArrayList<Map<String, Object>>();
        int i = 0;
        for (SearchHit hit : searchResponse.getHits().hits()) {
            Map<String, Object> source = new HashMap<String, Object>();
            String riverName = hit.getType();
            RiverSettings riverSettings = new RiverSettings(null, hit.getSource());
            MongoDBRiverDefinition definition = MongoDBRiverDefinition.parseSettings(riverName, riverIndexName, riverSettings, null);

            Timestamp<?> ts = MongoDBRiver.getLastTimestamp(esClient, definition);
            Long lastTimestamp = null;
            if (ts != null) {
                lastTimestamp = ts.getTime();
            }
            source.put("name", riverName);
            source.put("status", MongoDBRiverHelper.getRiverStatus(esClient, riverName));
            source.put("settings", hit.getSource());
            source.put("lastTimestamp", lastTimestamp);
            source.put("indexCount", MongoDBRiver.getIndexCount(esClient, definition));
            if (logger.isTraceEnabled()) {
                logger.trace("source: {}", hit.getSourceAsString());
            }
            rivers.add(source);
            i++;
        }
        data.put("count", i);
        data.put("results", rivers);
        return data;
    }

    private Map<String, Object> getRiver(String riverName, Client esClient) {
        if (!MongoDBRiverHelper.isRiverExist(esClient, riverName)) return null;
        SearchResponse searchResponse = esClient.prepareSearch(riverIndexName).setTypes(riverName)
                .setQuery(QueryBuilders.queryString(MongoDBRiver.TYPE).defaultField("type")).get();

        if (searchResponse.getHits().getTotalHits() > 0) {

            Map<String, Object> result = Maps.newHashMap();
            RiverSettings riverSettings = new RiverSettings(null, searchResponse.getHits().hits()[0].getSource());
            MongoDBRiverDefinition definition = MongoDBRiverDefinition.parseSettings(riverName, riverIndexName, riverSettings, null);

            Timestamp<?> ts = MongoDBRiver.getLastTimestamp(esClient, definition);
            Long lastTimestamp = null;
            if (ts != null) {
                lastTimestamp = ts.getTime();
            }
            result.put("name", riverName);
            result.put("status", MongoDBRiverHelper.getRiverStatus(esClient, riverName));
            result.put("settings", searchResponse.getHits().hits()[0].getSource());
            result.put("lastTimestamp", lastTimestamp);
            result.put("indexCount", MongoDBRiver.getIndexCount(esClient, definition));
            if (logger.isTraceEnabled()) {
                logger.trace("source: {}", searchResponse.getHits().hits()[0].getSourceAsString());
            }
            return result;
        }
        return null;
    }


}
