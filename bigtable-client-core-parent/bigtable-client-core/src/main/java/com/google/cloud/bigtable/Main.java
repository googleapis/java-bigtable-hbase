package com.google.cloud.bigtable;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CallOptionsConfig;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        Server server = ServerBuilder.forPort(1234)
                .addService(
                        new BigtableGrpc.BigtableImplBase() {
                            @Override
                            public void mutateRow(MutateRowRequest request, StreamObserver<MutateRowResponse> responseObserver) {
                                try {
                                    Thread.sleep(110);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                               // responseObserver.onError(new StatusRuntimeException(Status.DEADLINE_EXCEEDED));
//
//                                responseObserver.onNext(MutateRowResponse.getDefaultInstance());
//                                responseObserver.onCompleted();
                            }
                        }
                )
                .build();

        server.start();

        BigtableOptions opts = BigtableOptions.builder()
                .enableEmulator("localhost", 1234)
                .setUserAgent("moo")
                .setProjectId("fake-project")
                .setInstanceId("fake-instance")
                .setRetryOptions(
                        RetryOptions.builder().setRetryOnDeadlineExceeded(true).setEnableRetries(true).build())
                .setCallOptionsConfig(CallOptionsConfig.builder().setUseTimeout(true).setMutateRpcTimeoutMs(1400).setShortRpcTimeoutMs(4000).build())
                .build();

        BigtableSession session = new BigtableSession(opts);
        session.getDataClient().mutateRow(MutateRowRequest.getDefaultInstance());

    }
}

