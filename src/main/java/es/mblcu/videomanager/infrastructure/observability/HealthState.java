package es.mblcu.videomanager.infrastructure.observability;

import java.util.concurrent.atomic.AtomicBoolean;

public class HealthState {

    private final AtomicBoolean liveness = new AtomicBoolean(false);
    private final AtomicBoolean readiness = new AtomicBoolean(false);
    private final AtomicBoolean startup = new AtomicBoolean(false);

    public boolean isLivenessUp() {
        return liveness.get();
    }

    public boolean isReadinessUp() {
        return readiness.get();
    }

    public boolean isStartupUp() {
        return startup.get();
    }

    public void markLive(boolean value) {
        liveness.set(value);
    }

    public void markReady(boolean value) {
        readiness.set(value);
    }

    public void markStarted(boolean value) {
        startup.set(value);
    }

}
