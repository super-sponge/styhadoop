package com.sponge.srd.flume;


import java.util.HashMap;
import java.util.Map;

/**
 * Created by sponge on 15-2-10.
 */
public class MyApp {
    public static void main(String[] args) {
        MyRpcClientFacade client = new MyRpcClientFacade();
        // Initialize client with the remote Flume agent's host and port
        client.init("127.0.0.1", 41414);

        // Send 10 events to the remote Flume agent. That agent should be
        // configured to listen with an AvroSource.
        String sampleData = "Hello Flume!";
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("head1","time");
        headers.put("head2","time");


        for (int i = 0; i < 10; i++) {
            client.sendDataToFlume(sampleData,headers);
        }

        client.cleanUp();
    }
}
