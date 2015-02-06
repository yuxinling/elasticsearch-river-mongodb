package org.elasticsearch.river.mongodb;

public enum Status {

    UNKNOWN,
    START_PENDING,   //River should be started
    STARTING,        //River is actually starting up
    RUNNING,
    RESTART,
    STOPPED,

//    START_FAILED,
//    IMPORT_FAILED
    INTERRUPTED

}


