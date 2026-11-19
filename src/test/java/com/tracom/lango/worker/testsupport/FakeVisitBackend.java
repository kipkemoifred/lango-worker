package com.tracom.lango.worker.testsupport;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.tracom.lango.proto.v1.MarkVisitMissedRequest;
import com.tracom.lango.proto.v1.RemindVisitDueRequest;
import com.tracom.lango.proto.v1.ScheduleVisitRequest;
import com.tracom.lango.proto.v1.ScheduleVisitResponse;
import com.tracom.lango.proto.v1.VisitCallbackResponse;
import com.tracom.lango.proto.v1.VisitCallbackServiceGrpc;

import io.grpc.stub.StreamObserver;

/** Stands in for the Go backend's visit callbacks (same port 9090) and records every call. */
public class FakeVisitBackend extends VisitCallbackServiceGrpc.VisitCallbackServiceImplBase {

    public final List<ScheduleVisitRequest> scheduled = new CopyOnWriteArrayList<>();
    public final List<RemindVisitDueRequest> reminders = new CopyOnWriteArrayList<>();
    public final List<MarkVisitMissedRequest> missed = new CopyOnWriteArrayList<>();

    /** When true, the backend answers success=false, like a failed DB write. */
    public volatile boolean rejectRequests = false;

    @Override
    public void scheduleVisit(ScheduleVisitRequest request, StreamObserver<ScheduleVisitResponse> responseObserver) {
        scheduled.add(request);
        responseObserver.onNext(ScheduleVisitResponse.newBuilder()
                .setSuccess(!rejectRequests)
                // like the real backend: the same (cycle, due date) always gives the same visit
                .setVisitId("visit-" + request.getCycleId() + "-" + request.getDueDate())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void remindVisitDue(RemindVisitDueRequest request, StreamObserver<VisitCallbackResponse> responseObserver) {
        reminders.add(request);
        reply(responseObserver);
    }

    @Override
    public void markVisitMissed(MarkVisitMissedRequest request, StreamObserver<VisitCallbackResponse> responseObserver) {
        missed.add(request);
        reply(responseObserver);
    }

    private void reply(StreamObserver<VisitCallbackResponse> responseObserver) {
        responseObserver.onNext(VisitCallbackResponse.newBuilder().setSuccess(!rejectRequests).build());
        responseObserver.onCompleted();
    }
}
