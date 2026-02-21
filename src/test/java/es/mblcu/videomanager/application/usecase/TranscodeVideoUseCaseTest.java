package es.mblcu.videomanager.application.usecase;

import es.mblcu.videomanager.application.service.LocalWorkspaceService;
import es.mblcu.videomanager.application.service.TranscodeProfileCatalogService;
import es.mblcu.videomanager.domain.FileRepository;
import es.mblcu.videomanager.domain.jobs.JobStateRepository;
import es.mblcu.videomanager.domain.jobs.vo.JobStatus;
import es.mblcu.videomanager.domain.transcode.TranscodeProfile;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoCommand;
import es.mblcu.videomanager.domain.transcode.TranscodeVideoResult;
import es.mblcu.videomanager.domain.transcode.VideoTranscodingPort;
import es.mblcu.videomanager.domain.transcode.exception.TranscodeValidationException;
import es.mblcu.videomanager.domain.transcode.jobs.TranscodeJobState;
import es.mblcu.videomanager.domain.transcode.jobs.TranscodeJobStateRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TranscodeVideoUseCaseTest {

    @TempDir
    Path tempDir;

    private LocalWorkspaceService workspace;

    @Mock
    private VideoTranscodingPort transcodingPort;

    @Mock
    private FileRepository fileRepository;

    @Mock
    private JobStateRepository videoRefRepository;

    @Mock
    private TranscodeJobStateRepository transcodeJobStateRepository;

    private TranscodeVideoUseCase useCase;

    @BeforeEach
    void setUp() {
        workspace = new LocalWorkspaceService(tempDir);
        final var catalog = new TranscodeProfileCatalogService(Map.of(
            "1280x720", List.of(
                new TranscodeProfile("854x480", 854, 480, 1400),
                new TranscodeProfile("640x360", 640, 360, 800),
                new TranscodeProfile("426x240", 426, 240, 400)
            )
        ));

        useCase = new TranscodeVideoUseCase(
            transcodingPort,
            fileRepository,
            workspace,
            videoRefRepository,
            transcodeJobStateRepository,
            catalog
        );
    }

    @Test
    void should_fail_when_dimensions_are_unsupported() {
        final var command = new TranscodeVideoCommand(
            1L,
            "s3://bucket/videos/in.mp4",
            "s3://bucket/transcoded/1",
            1111,
            777
        );

        assertThatThrownBy(() -> useCase.execute(command).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(TranscodeValidationException.class);

        verify(transcodeJobStateRepository, never()).markRunning(any(), any());
    }

    @Test
    void should_download_transcode_upload_and_cleanup_local_files() {
        final var command = new TranscodeVideoCommand(
            10L,
            "s3://bucket/videos/video.mp4",
            "s3://bucket/transcoded/10",
            1280,
            720
        );
        String jobId = "10|s3://bucket/transcoded/10|1280x720";
        final var uploads = new AtomicInteger();

        when(transcodeJobStateRepository.findJob(jobId)).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(transcodeJobStateRepository.markRunning(eq(jobId), eq(command))).thenReturn(CompletableFuture.completedFuture(null));
        when(transcodeJobStateRepository.markSuccess(eq(jobId), any(TranscodeVideoResult.class))).thenReturn(CompletableFuture.completedFuture(null));

        when(videoRefRepository.incrementVideoRef(eq(command.videoS3Path()))).thenReturn(CompletableFuture.completedFuture(1L));
        when(videoRefRepository.decrementVideoRef(eq(command.videoS3Path()))).thenReturn(CompletableFuture.completedFuture(0L));

        when(fileRepository.download(eq(command.videoS3Path()), any(Path.class))).thenAnswer(invocation -> {
            Path localVideo = invocation.getArgument(1);
            Files.createDirectories(localVideo.getParent());
            Files.writeString(localVideo, "video-bytes");
            return CompletableFuture.completedFuture(null);
        });
        when(transcodingPort.transcode(any(Path.class), any(List.class))).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            List<es.mblcu.videomanager.domain.transcode.TranscodeTarget> targets = invocation.getArgument(1);
            for (var target : targets) {
                Files.createDirectories(target.localOutputFile().getParent());
                Files.writeString(target.localOutputFile(), "out-" + target.profile().name());
            }
            return CompletableFuture.completedFuture(Duration.ofMillis(50));
        });
        when(fileRepository.upload(any(Path.class), any(String.class))).thenAnswer(invocation -> {
            uploads.incrementAndGet();
            Path localOutput = invocation.getArgument(0);
            assertThat(Files.exists(localOutput)).isTrue();
            return CompletableFuture.completedFuture(null);
        });

        final var result = useCase.execute(command).join();

        assertThat(result.videoId()).isEqualTo(10L);
        assertThat(result.outputs()).containsKeys("854x480", "640x360", "426x240");
        assertThat(result.outputs().get("854x480")).isEqualTo("s3://bucket/transcoded/10/854x480.mp4");
        assertThat(uploads.get()).isEqualTo(3);

        Path localVideo = workspace.resolveLocalVideoPath(command.videoS3Path());
        await().untilAsserted(() -> assertThat(Files.exists(localVideo)).isFalse());
    }

    @Test
    void should_be_idempotent_when_job_already_succeeded() {
        final var command = new TranscodeVideoCommand(
            11L,
            "s3://bucket/videos/video.mp4",
            "s3://bucket/transcoded/11",
            1280,
            720
        );
        String jobId = "11|s3://bucket/transcoded/11|1280x720";
        final var state = new TranscodeJobState(
            jobId,
            11L,
            command.videoS3Path(),
            command.outputS3Prefix(),
            1280,
            720,
            JobStatus.SUCCESS,
            100L,
            null,
            Map.of("854x480", "s3://bucket/transcoded/11/854x480.mp4")
        );

        when(transcodeJobStateRepository.findJob(jobId)).thenReturn(CompletableFuture.completedFuture(Optional.of(state)));

        final var result = useCase.execute(command).join();

        assertThat(result.videoId()).isEqualTo(11L);
        assertThat(result.outputs()).containsEntry("854x480", "s3://bucket/transcoded/11/854x480.mp4");
        verify(fileRepository, never()).download(any(), any());
        verify(transcodingPort, never()).transcode(any(), any());
    }

}
