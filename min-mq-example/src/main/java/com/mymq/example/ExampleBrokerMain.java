package com.minmq.example;

import com.minmq.broker.BrokerServer;

public class ExampleBrokerMain {
    public static void main(String[] args) throws Exception {
        new BrokerServer(8080, 8081, "./data").start();
    }
}