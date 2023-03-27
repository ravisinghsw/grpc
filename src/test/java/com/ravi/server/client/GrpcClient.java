package com.ravi.server.client;

import com.ravi.server.gprc.HelloRequest;
import com.ravi.server.gprc.HelloResponse;
import com.ravi.server.gprc.HelloWorldServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

public class GrpcClient {

    public static void main(String[] args) throws InterruptedException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();

        HelloWorldServiceGrpc.HelloWorldServiceBlockingStub stub = HelloWorldServiceGrpc.newBlockingStub(channel);

        HelloResponse helloResponse = stub.hello(HelloRequest.newBuilder()
                .setText("Ravi")
                .build());
        System.out.println("response from server"+helloResponse.getText());

        channel.awaitTermination(10, TimeUnit.SECONDS);
        if(!channel.isShutdown()){
            channel.shutdownNow();
        }




    }
}
