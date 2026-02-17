package es.mblcu.videomanager.domain;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public interface FileRepository {

    CompletableFuture<Void> download(String sourceS3Path, Path localTargetFile);

    CompletableFuture<Void> upload(Path localSourceFile, String targetS3Path);
}
