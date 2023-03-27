package com.ravi.server.client;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;


import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ArrowClient {

    private static final Logger LOGGER = Logger.getLogger(String.valueOf(ArrowClient.class));
    private String outpath;
    public ArrowClient(int threads , String outpath) throws InterruptedException {
        LOGGER.info("ParallelArrow Client Started");
        this.outpath = outpath;
        recieveData(threads);
    }
    class ClientNode implements Runnable {
        public void run() {
            long start = System.currentTimeMillis();
            try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                 FlightClient client = FlightClient
                         .builder(allocator, Location.forGrpcInsecure("localhost", 5000)).build();
                 FlightStream stream = client.getStream(new Ticket(new byte[] {}),
                         CallOptions.timeout(500, TimeUnit.DAYS));) {
                LOGGER.info("started data exchanging....");
                while (stream.next()) {
                    System.out.println(stream.getDescriptor().toString());

                }
                LOGGER.info("finished");
            } catch (Exception e) {
                e.printStackTrace();
            }
            long end = System.currentTimeMillis();
            LOGGER.info(String.format("elapsed time : %f secs", ((double) (end - start) / 1000)));
        }
    }
    private void recieveData(int threads) throws InterruptedException {
        if (threads == 1) {
            new ClientNode().run();
        } else
            for (int i = 0; i < threads; i++) {
                (new Thread(new ClientNode())).start();
            }
    }
    public static void main(String[] args) throws InterruptedException {
        new ArrowClient(1, "output.arrow");
    }
}
