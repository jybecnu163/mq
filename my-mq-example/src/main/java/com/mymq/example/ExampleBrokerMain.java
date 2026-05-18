package com.mymq.example;

import com.mymq.broker.BrokerServer;

public class ExampleBrokerMain {
    public static void main(String[] args) throws Exception {
        new BrokerServer(8080, 8081, "./data").start();
    }
}