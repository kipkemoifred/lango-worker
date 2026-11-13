package com.tracom.lango.worker.testsupport;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.tracom.lango.proto.v1.RecordSlaBreachRequest;
import com.tracom.lango.proto.v1.RecordSlaBreachResponse;
import com.tracom.lango.proto.v1.SyncPausedSlaDurationRequest;
import com.tracom.lango.proto.v1.SyncPausedSlaDurationResponse;
import com.tracom.lango.proto.v1.TicketInternalCallbackServiceGrpc;

import io.grpc.stub.StreamObserver;

/** Stands in for the Go backend's internal gRPC server (port 9090) and records every call. */
public class FakeGoBackend extends TicketInternalCallbackServiceGrpc.TicketInternalCallbackServiceImplBase {

    public final List<RecordSlaBreachRequest> breaches = new CopyOnWriteArrayList<>();
    public final List<SyncPausedSlaDurationRequest> pauseSyncs = new CopyOnWriteArrayList<>();

    /** When true, the backend answers success=false, like a failed DB write. */
    public volatile boolean rejectRequests = false;

    @Override
    public void recordSlaBreachAndEscalate(RecordSlaBreachRequest request,
            StreamObserver<RecordSlaBreachResponse> responseObserver) {
        breaches.add(request);
        responseObserver.onNext(RecordSlaBreachResponse.newBuilder()
                .setSuccess(!rejectRequests)
                .setCurrentTicketStatus("IN_PROGRESS")
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void syncPausedSlaDuration(SyncPausedSlaDurationRequest request,
            StreamObserver<SyncPausedSlaDurationResponse> responseObserver) {
        pauseSyncs.add(request);
        responseObserver.onNext(SyncPausedSlaDurationResponse.newBuilder()
                .setSuccess(!rejectRequests)
                .build());
        responseObserver.onCompleted();
    }
}
