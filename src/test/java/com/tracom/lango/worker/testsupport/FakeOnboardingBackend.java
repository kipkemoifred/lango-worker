package com.tracom.lango.worker.testsupport;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.tracom.lango.proto.v1.DeclineOnboardingRequest;
import com.tracom.lango.proto.v1.OnboardingCallbackResponse;
import com.tracom.lango.proto.v1.OnboardingCallbackServiceGrpc;
import com.tracom.lango.proto.v1.OnboardingStageOverdueRequest;
import com.tracom.lango.proto.v1.ProvisionMerchantRequest;
import com.tracom.lango.proto.v1.ProvisionMerchantResponse;

import io.grpc.stub.StreamObserver;

/** Stands in for the Go backend's onboarding callbacks (same port 9090) and records every call. */
public class FakeOnboardingBackend extends OnboardingCallbackServiceGrpc.OnboardingCallbackServiceImplBase {

    public final List<OnboardingStageOverdueRequest> overdueAlerts = new CopyOnWriteArrayList<>();
    public final List<DeclineOnboardingRequest> declines = new CopyOnWriteArrayList<>();
    public final List<ProvisionMerchantRequest> provisions = new CopyOnWriteArrayList<>();

    /** When true, the backend answers success=false, like a failed DB write or TMS call. */
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

    @Override
    public void provisionMerchant(ProvisionMerchantRequest request,
            StreamObserver<ProvisionMerchantResponse> responseObserver) {
        provisions.add(request);
        responseObserver.onNext(ProvisionMerchantResponse.newBuilder()
                .setSuccess(!rejectRequests)
                .setMerchantId("merchant-" + request.getApplicationId())
                .setOutletId("outlet-" + request.getApplicationId())
                .setMid("MID-" + request.getApplicationId())
                .build());
        responseObserver.onCompleted();
    }

    private void reply(StreamObserver<OnboardingCallbackResponse> responseObserver) {
        responseObserver.onNext(OnboardingCallbackResponse.newBuilder().setSuccess(!rejectRequests).build());
        responseObserver.onCompleted();
    }
}
