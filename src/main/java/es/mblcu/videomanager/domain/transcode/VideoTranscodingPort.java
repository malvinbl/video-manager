package es.mblcu.videomanager.domain.transcode;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface VideoTranscodingPort {

    CompletableFuture<Duration> transcode(Path localVideoFile, List<TranscodeTarget> targets);

}
