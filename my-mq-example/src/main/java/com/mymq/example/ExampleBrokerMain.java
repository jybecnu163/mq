package com.mymq.example;

import com.mymq.broker.BrokerServer;

public class ExampleBrokerMain {
    public static void main(String[] args) throws InterruptedException {
        new BrokerServer(8080).start();
    }
}