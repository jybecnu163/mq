package com.minmq.broker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.InputStream;

public class BrokerConfig {
    private Broker broker = new Broker();
    private Logging logging = new Logging();

    public Broker getBroker() {
        return broker;
    }

    public void setBroker(Broker broker) {
        this.broker = broker;
    }

    public Logging getLogging() {
        return logging;
    }

    public void setLogging(Logging logging) {
        this.logging = logging;
    }

    public static class Broker {
        private int port = 8080;
        private int metricsPort = 8081;
        private String dataDir = "./data";

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public int getMetricsPort() {
            return metricsPort;
        }

        public void setMetricsPort(int metricsPort) {
            this.metricsPort = metricsPort;
        }

        public String getDataDir() {
            return dataDir;
        }

        public void setDataDir(String dataDir) {
            this.dataDir = dataDir;
        }
    }

    public static class Logging {
        private String level = "INFO";

        public String getLevel() {
            return level;
        }

        public void setLevel(String level) {
            this.level = level;
        }
    }

    public static BrokerConfig load() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try (InputStream in = BrokerConfig.class.getResourceAsStream("/config.yaml")) {
            if (in != null) {
                return mapper.readValue(in, BrokerConfig.class);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new BrokerConfig(); // 默认配置
    }
}