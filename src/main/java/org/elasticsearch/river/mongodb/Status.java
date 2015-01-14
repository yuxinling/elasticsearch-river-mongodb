package org.elasticsearch.river.mongodb;

public enum Status {

    UNKNOWN,
    /**
     * River should be started
     */
    START_PENDING,
    /**
     * River is actually starting up
     */
    STARTING,
    START_FAILED,
    RUNNING,
    RESTART,
    STOPPED,
    IMPORT_FAILED,

    //INITIAL_IMPORT_FAILED,
    //SCRIPT_IMPORT_FAILED,
    //RIVER_STALE,

    COLLECTION_DROPED,

    MONGO_UPDATE,
    MONGO_NORMAL

}
