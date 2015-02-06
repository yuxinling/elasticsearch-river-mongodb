package org.elasticsearch.river.mongodb.ds;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * User: yuyangning
 * Date: 2/5/15
 * Time: 4:03 PM
 */
public class ServerParser {

    public static List<Server> parse(String server) {
        String[] servers = server.split(",");
        List<Server> ss = Lists.newArrayList();
        for (String s : servers) {
            ss.add(new Server(s));
        }
        return ss;
    }
}
