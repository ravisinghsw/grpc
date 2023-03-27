package com.ravi.server.grpc;


import com.ravi.server.gprc.Hello;
import com.ravi.server.gprc.HelloRequest;
import com.ravi.server.gprc.HelloResponse;
import com.ravi.server.gprc.HelloWorldServiceGrpc;
import io.grpc.stub.StreamObserver;

public class HelloWorldService extends HelloWorldServiceGrpc.HelloWorldServiceImplBase {

    @Override
    public void hello(
            HelloRequest request,
            StreamObserver<HelloResponse> responseObserver) {
        System.out.println(
                "Handling hello endpoint: " + request.toString());


        String text = request.getText() + " World";
       HelloResponse response =
                HelloResponse.newBuilder()
                        .setText(text).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}
