import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.tracom.lango.proto.v1.*;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;

/**
 * Manual end-to-end testing only: stands in for the Go backend's internal gRPC server and
 * prints every callback the worker makes. Answers success=true, or success=false when
 * STUB_REJECT=true (to watch Temporal retry). Not part of the build.
 *
 * Run from the project root after ./mvnw package -DskipTests (see e2e/MANUAL-E2E.md):
 *   java -cp "target/classes:target/quarkus-app/lib/main/*" e2e/StubGoBackend.java
 */
public class StubGoBackend {

    static final boolean REJECT = Boolean.parseBoolean(System.getenv().getOrDefault("STUB_REJECT", "false"));

    static void log(String rpc, Message request) {
        System.out.printf("%s  %-30s %s%s%n", LocalTime.now().withNano(0), rpc,
                TextFormat.printer().emittingSingleLine(true).printToString(request), REJECT ? "  -> success=false" : "");
    }

    static class Tickets extends TicketInternalCallbackServiceGrpc.TicketInternalCallbackServiceImplBase {
        @Override
        public void recordSlaBreachAndEscalate(RecordSlaBreachRequest req, StreamObserver<RecordSlaBreachResponse> out) {
            log("RecordSlaBreachAndEscalate", req);
            out.onNext(RecordSlaBreachResponse.newBuilder().setSuccess(!REJECT).setCurrentTicketStatus("IN_PROGRESS").build());
            out.onCompleted();
        }

        @Override
        public void recordSlaWarning(RecordSlaWarningRequest req, StreamObserver<RecordSlaWarningResponse> out) {
            log("RecordSlaWarning", req);
            out.onNext(RecordSlaWarningResponse.newBuilder().setSuccess(!REJECT).build());
            out.onCompleted();
        }

        @Override
        public void syncPausedSlaDuration(SyncPausedSlaDurationRequest req, StreamObserver<SyncPausedSlaDurationResponse> out) {
            log("SyncPausedSlaDuration", req);
            out.onNext(SyncPausedSlaDurationResponse.newBuilder().setSuccess(!REJECT).build());
            out.onCompleted();
        }
    }

    static class Onboarding extends OnboardingCallbackServiceGrpc.OnboardingCallbackServiceImplBase {
        @Override
        public void notifyOnboardingStageOverdue(OnboardingStageOverdueRequest req, StreamObserver<OnboardingCallbackResponse> out) {
            log("NotifyOnboardingStageOverdue", req);
            out.onNext(OnboardingCallbackResponse.newBuilder().setSuccess(!REJECT).build());
            out.onCompleted();
        }

        @Override
        public void declineOnboardingApplication(DeclineOnboardingRequest req, StreamObserver<OnboardingCallbackResponse> out) {
            log("DeclineOnboardingApplication", req);
            out.onNext(OnboardingCallbackResponse.newBuilder().setSuccess(!REJECT).build());
            out.onCompleted();
        }
    }

    static class Visits extends VisitCallbackServiceGrpc.VisitCallbackServiceImplBase {
        @Override
        public void scheduleVisit(ScheduleVisitRequest req, StreamObserver<ScheduleVisitResponse> out) {
            log("ScheduleVisit", req);
            // like the real backend: the same (cycle, due date) always gives the same visit
            out.onNext(ScheduleVisitResponse.newBuilder().setSuccess(!REJECT)
                    .setVisitId("visit-" + req.getCycleId() + "-" + req.getDueDate()).build());
            out.onCompleted();
        }

        @Override
        public void remindVisitDue(RemindVisitDueRequest req, StreamObserver<VisitCallbackResponse> out) {
            log("RemindVisitDue", req);
            out.onNext(VisitCallbackResponse.newBuilder().setSuccess(!REJECT).build());
            out.onCompleted();
        }

        @Override
        public void markVisitMissed(MarkVisitMissedRequest req, StreamObserver<VisitCallbackResponse> out) {
            log("MarkVisitMissed", req);
            out.onNext(VisitCallbackResponse.newBuilder().setSuccess(!REJECT).build());
            out.onCompleted();
        }
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("STUB_PORT", "9090"));
        Server server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(new Tickets())
                .addService(new Onboarding())
                .addService(new Visits())
                .build()
                .start();
        System.out.printf("Stub Go backend listening on :%d (reject=%s). Ctrl+C to stop.%n", port, REJECT);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.shutdown()));
        server.awaitTermination();
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
}
