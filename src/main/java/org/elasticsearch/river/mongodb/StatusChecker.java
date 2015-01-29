package org.elasticsearch.river.mongodb;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;

import java.util.Map;

class StatusChecker implements Runnable {
    private static final ESLogger logger = ESLoggerFactory.getLogger(StatusChecker.class.getName());

    private final MongoDBRiver mongoDBRiver;
    private final MongoDBRiverDefinition definition;
    private final SharedContext context;

    public StatusChecker(MongoDBRiver mongoDBRiver, MongoDBRiverDefinition definition, SharedContext context) {
        this.mongoDBRiver = mongoDBRiver;
        this.definition = definition;
        this.context = context;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Status status = MongoDBRiverHelper.getRiverStatus(this.mongoDBRiver.esClient, this.definition.getRiverName());
                //logger.info("The river {} status is {}, Context status is {}", this.definition.getRiverName(), status, this.context.getStatus());
                if (status != this.context.getStatus()) {

                    if (status == Status.RUNNING && this.context.getStatus() != Status.STARTING) {
                        logger.info("About to start river: {}", this.definition.getRiverName());
                        this.mongoDBRiver.internalStartRiver();
                    } else if (status == Status.STOPPED) {
                        logger.info("About to stop river: {}", this.definition.getRiverName());
                        this.mongoDBRiver.internalStopRiver();
                    } else if (status == Status.IMPORT_FAILED || status == Status.COLLECTION_DROPED) {
                        logger.info("The river {} status is {} ,River close and status thread interrupted.", this.definition.getRiverName(), status);
                        this.mongoDBRiver.close();
                    } else if (status == Status.RESTART) {
                        this.mongoDBRiver.internalRestarRiver();
                    }

                } else {
                    if (status == Status.RUNNING && this.context.getStatus() != Status.STARTING) {
                        Status mongoStatus = MongoDBRiverHelper.getMongoStatus(this.mongoDBRiver.esClient, this.definition.getRiverName());
                        if (mongoStatus == Status.MONGO_UPDATE) {
                            Map<String, Object> setting = MongoDBRiverHelper.getSetting(this.mongoDBRiver.esClient, this.definition.getRiverName());
                            if (setting != null) {
                                this.mongoDBRiver.internalRestarRiver(setting);
                            } else {
                                logger.error("The river[{}] setting is empty. closing...", this.definition.getRiverName());
                                this.mongoDBRiver.close();
                            }
                        }
                    } else if (status == Status.RESTART) {
                        this.mongoDBRiver.internalRestarRiver();
                    }
                }

                Thread.sleep(10_000L);
            } catch (InterruptedException e) {
                logger.debug("Status thread interrupted", e, (Object) null);
                Thread.currentThread().interrupt();
                break;
            }

        }
    }
}
