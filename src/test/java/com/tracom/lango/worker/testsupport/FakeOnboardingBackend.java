package com.tracom.lango.worker.testsupport;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.tracom.lango.proto.v1.DeclineOnboardingRequest;
import com.tracom.lango.proto.v1.OnboardingCallbackResponse;
import com.tracom.lango.proto.v1.OnboardingCallbackServiceGrpc;
import com.tracom.lango.proto.v1.OnboardingStageOverdueRequest;

import io.grpc.stub.StreamObserver;

/** Stands in for the Go backend's onboarding callbacks (same port 9090) and records every call. */
public class FakeOnboardingBackend extends OnboardingCallbackServiceGrpc.OnboardingCallbackServiceImplBase {

    public final List<OnboardingStageOverdueRequest> overdueAlerts = new CopyOnWriteArrayList<>();
    public final List<DeclineOnboardingRequest> declines = new CopyOnWriteArrayList<>();

    /** When true, the backend answers success=false, like a failed DB write. */
    public volatile boolean rejectRequests = false;

    @Override
    public void notifyOnboardingStageOverdue(OnboardingStageOverdueRequest request,
            StreamObserver<OnboardingCallbackResponse> responseObserver) {
        overdueAlerts.add(request);
        reply(responseObserver);
    }

    @Override
    public void declineOnboardingApplication(DeclineOnboardingRequest request,
            StreamObserver<OnboardingCallbackResponse> responseObserver) {
        declines.add(request);
        reply(responseObserver);
    }

    private void reply(StreamObserver<OnboardingCallbackResponse> responseObserver) {
        responseObserver.onNext(OnboardingCallbackResponse.newBuilder().setSuccess(!rejectRequests).build());
        responseObserver.onCompleted();
    }
}
