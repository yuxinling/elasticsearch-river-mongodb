package org.elasticsearch.river.mongodb;

import com.google.common.collect.Maps;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;

import java.util.Map;

/**
 * User: yuyangning
 * Date: 2/6/15
 * Time: 12:24 PM
 */
public class StatusCheckerProxy implements Runnable {
    private static final ESLogger logger = ESLoggerFactory.getLogger(StatusChecker.class.getName());

    private final Map<String, MongoDBRiver> rivers = Maps.newConcurrentMap();
    private static StatusCheckerProxy proxy = null;

    private StatusCheckerProxy() {

    }

    public static StatusCheckerProxy instance() {
        if (proxy == null) {
            synchronized (StatusCheckerProxy.class) {
                if (proxy == null) {
                    proxy = new StatusCheckerProxy();
                    EsExecutors.daemonThreadFactory("mongodb_river_status").newThread(proxy).start();
                }
            }
        }
        return proxy;
    }

    public void addRiverCheck(MongoDBRiver mongoDBRiver) {
        if (mongoDBRiver == null) return;
        rivers.put(mongoDBRiver.definition.getRiverName(), mongoDBRiver);
    }

    public void removeRiverCheck(MongoDBRiver mongoDBRiver) {
        if (mongoDBRiver == null) return;
        if (rivers.containsKey(mongoDBRiver.definition.getRiverName())) {
            rivers.remove(mongoDBRiver.definition.getRiverName());
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                for (Map.Entry<String, MongoDBRiver> entry : rivers.entrySet()) {
                    MongoDBRiver mongoDBRiver = entry.getValue();
                    try {
                        Status status = MongoDBRiverHelper.getRiverStatus(mongoDBRiver.esClient, mongoDBRiver.definition.getRiverName());
                        Status contextStatus = mongoDBRiver.context.getStatus();

                        if (status != mongoDBRiver.context.getStatus()) {
                            if (status == Status.RUNNING && contextStatus != Status.STARTING) {
                                logger.trace("The river {} start.", mongoDBRiver.definition.getRiverName());
                                mongoDBRiver.internalStartRiver();
                            } else if (status == Status.STOPPED && contextStatus != Status.INTERRUPTED) {
                                logger.info("The river {} stop.", mongoDBRiver.definition.getRiverName());
                                mongoDBRiver.internalStopRiver();
                            } else if (status == Status.INTERRUPTED && contextStatus != Status.STOPPED) {
                                logger.info("The river {} status is {} ,River close and status thread interrupted.", mongoDBRiver.definition.getRiverName(), status);
                                mongoDBRiver.internalStopRiver();
                            } else if (status == Status.RESTART) {
                                logger.trace("The river {} restart.", mongoDBRiver.definition.getRiverName());
                                mongoDBRiver.internalRestarRiver();
                            }
                        }
                    } catch (Exception e) {
                        logger.error("The river[{}] error.", mongoDBRiver.riverName().getName(), e);
                    }
                }
                Thread.sleep(3_000L);
            } catch (InterruptedException e) {
                logger.debug("Status thread interrupted", e, (Object) null);
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
