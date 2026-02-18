package es.mblcu.videomanager.domain.frame;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public interface FrameExtractionPort {

    CompletableFuture<Duration> extractFrame(Path localVideoFile, Path localFrameFile, double second);

}
