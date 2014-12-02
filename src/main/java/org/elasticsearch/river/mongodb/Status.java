package org.elasticsearch.river.mongodb;

public enum Status {

    UNKNOWN,

    START_FAILED,
    IMPORT_FAILED,

    RUNNING,
    STOPPED,

//    INITIAL_IMPORT_FAILED,
//    SCRIPT_IMPORT_FAILED,
//    RIVER_STALE,

    COLLECTION_DROPED,

    MONGO_UPDATE,
    MONGO_NORMAL;

}
