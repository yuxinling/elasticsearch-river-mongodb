package org.elasticsearch.river.mongodb.ds;

public class Server {
    private String host;
    private int port;

    public Server(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Server(String server) {
        //10.20.15.11:27017
        String[] s = server.split(":");
        this.host = s[0];
        this.port = Integer.valueOf(s[1]);
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}