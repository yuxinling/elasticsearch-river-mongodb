package org.elasticsearch.river.mongodb;

import org.elasticsearch.river.mongodb.util.MongoDBRiverHelper;

import java.util.Map;

class StatusChecker implements Runnable {

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
                if (this.mongoDBRiver.startInvoked) {
                    Status status = MongoDBRiverHelper.getRiverStatus(this.mongoDBRiver.esClient, this.definition.getRiverName());
                    if (status == Status.RUNNING) {
                        if (status != this.context.getStatus()) {
                            this.mongoDBRiver.start();
                        } else {
                            Status mongoStatus = MongoDBRiverHelper.getMongoStatus(this.mongoDBRiver.esClient, this.definition.getRiverName());
                            if (mongoStatus == Status.MONGO_UPDATE) {
                                Map<String, Object> setting = MongoDBRiverHelper.getSetting(this.mongoDBRiver.esClient, this.definition.getRiverName());
                                if (setting != null) {
                                    this.mongoDBRiver.restartWithSetting(setting);
                                } else {
                                    MongoDBRiver.logger.error("The river[{}] setting is empty. closing...", this.definition.getRiverName());
                                    this.mongoDBRiver.close();
                                }
                            }
                        }
                    } else if (status == Status.STOPPED) {
                        if (status != this.context.getStatus()) {
                            this.mongoDBRiver.close();
                        }
                    } else if (status == Status.IMPORT_FAILED || status == Status.COLLECTION_DROPED) {
                        if (status != this.context.getStatus()) {
                            MongoDBRiver.logger.info("The river {} status is {} ,River close and status thread interrupted.", this.definition.getRiverName(), status);
                            this.mongoDBRiver.close();
                            Thread.currentThread().interrupt();
                        }
                    }
                }
                Thread.sleep(3000L);
            } catch (InterruptedException e) {
                MongoDBRiver.logger.debug("Status thread interrupted", e, (Object) null);
                Thread.currentThread().interrupt();
                break;
            }

        }
    }
}
