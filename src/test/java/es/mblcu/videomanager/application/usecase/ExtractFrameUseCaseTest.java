package es.mblcu.videomanager.application.usecase;

import es.mblcu.videomanager.application.service.LocalWorkspaceService;
import es.mblcu.videomanager.domain.FileRepository;
import es.mblcu.videomanager.domain.frame.ExtractFrameCommand;
import es.mblcu.videomanager.domain.frame.ExtractFrameResult;
import es.mblcu.videomanager.domain.frame.FrameExtractionPort;
import es.mblcu.videomanager.domain.jobs.JobState;
import es.mblcu.videomanager.domain.jobs.JobStateRepository;
import es.mblcu.videomanager.domain.jobs.vo.JobStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExtractFrameUseCaseTest {

    @TempDir
    Path tempDir;

    private LocalWorkspaceService workspace;

    @Mock
    private FrameExtractionPort frameExtractionPort;

    @Mock
    private FileRepository fileRepository;

    @Mock
    private JobStateRepository jobStateRepository;

    private ExtractFrameUseCase useCase;

    @BeforeEach
    void setUp() {
        workspace = new LocalWorkspaceService(tempDir);
        useCase = new ExtractFrameUseCase(frameExtractionPort, fileRepository, workspace, jobStateRepository);
    }

    @Test
    void should_download_extract_upload_and_cleanup_local_files() {
        var downloads = new AtomicInteger();
        var uploads = new AtomicInteger();
        final var command = command(101L, "s3://bucket/frames/frame.png");

        stubJobRepositoryForNormalFlow(command);
        when(jobStateRepository.incrementVideoRef(eq(command.videoS3Path()))).thenReturn(CompletableFuture.completedFuture(1L));
        when(jobStateRepository.decrementVideoRef(eq(command.videoS3Path()))).thenReturn(CompletableFuture.completedFuture(0L));
        when(fileRepository.download(eq(command.videoS3Path()), any(Path.class))).thenAnswer(invocation -> {
            downloads.incrementAndGet();
            Path localVideo = invocation.getArgument(1);
            Files.createDirectories(localVideo.getParent());
            Files.writeString(localVideo, "video-bytes");
            return CompletableFuture.completedFuture(null);
        });
        when(frameExtractionPort.extractFrame(any(Path.class), any(Path.class), eq(command.second()))).thenAnswer(invocation -> {
            Path localVideo = invocation.getArgument(0);
            Path localFrame = invocation.getArgument(1);
            assertThat(Files.exists(localVideo)).isTrue();
            Files.writeString(localFrame, "frame-bytes");
            return CompletableFuture.completedFuture(Duration.ofMillis(12));
        });
        when(fileRepository.upload(any(Path.class), eq(command.frameS3Path()))).thenAnswer(invocation -> {
            uploads.incrementAndGet();
            Path localFrame = invocation.getArgument(0);
            assertThat(Files.exists(localFrame)).isTrue();
            return CompletableFuture.completedFuture(null);
        });

        final var result = useCase.execute(command).join();

        assertThat(downloads.get()).isEqualTo(1);
        assertThat(uploads.get()).isEqualTo(1);
        assertThat(result.videoId()).isEqualTo(101L);
        assertThat(result.frameS3Path()).isEqualTo("s3://bucket/frames/frame.png");
        assertThat(result.elapsed()).isEqualTo(Duration.ofMillis(12));

        final var localFrame = workspace.resolveLocalFramePath(command.frameS3Path());
        final var localVideo = workspace.resolveLocalVideoPath(command.videoS3Path());
        await().atMost(500, TimeUnit.MILLISECONDS).until(() -> !Files.exists(localFrame));
        await().atMost(500, TimeUnit.MILLISECONDS).until(() -> !Files.exists(localVideo));
    }

    @Test
    void should_be_idempotent_when_same_job_arrives_twice() {
        final var command = command(777L, "s3://bucket/frames/frame.png");
        String jobId = command.videoId() + "|" + command.frameS3Path() + "|" + command.second();
        var downloads = new AtomicInteger();
        var uploads = new AtomicInteger();
        var extractions = new AtomicInteger();

        when(jobStateRepository.findJob(jobId))
            .thenReturn(CompletableFuture.completedFuture(Optional.empty()))
            .thenReturn(CompletableFuture.completedFuture(Optional.of(
                new JobState(jobId, command.videoId(), command.videoS3Path(), command.frameS3Path(), JobStatus.SUCCESS, 8L, null)
            )));
        when(jobStateRepository.markRunning(eq(jobId), eq(command))).thenReturn(CompletableFuture.completedFuture(null));
        when(jobStateRepository.incrementVideoRef(eq(command.videoS3Path()))).thenReturn(CompletableFuture.completedFuture(1L));
        when(jobStateRepository.decrementVideoRef(eq(command.videoS3Path()))).thenReturn(CompletableFuture.completedFuture(0L));
        when(jobStateRepository.markSuccess(eq(jobId), any(ExtractFrameResult.class))).thenReturn(CompletableFuture.completedFuture(null));

        when(fileRepository.download(eq(command.videoS3Path()), any(Path.class))).thenAnswer(invocation -> {
            downloads.incrementAndGet();
            Path localVideo = invocation.getArgument(1);
            Files.createDirectories(localVideo.getParent());
            Files.writeString(localVideo, "video-bytes");
            return CompletableFuture.completedFuture(null);
        });
        when(frameExtractionPort.extractFrame(any(Path.class), any(Path.class), eq(command.second()))).thenAnswer(invocation -> {
            extractions.incrementAndGet();
            Path localFrame = invocation.getArgument(1);
            Files.writeString(localFrame, "frame-bytes");
            return CompletableFuture.completedFuture(Duration.ofMillis(8));
        });
        when(fileRepository.upload(any(Path.class), eq(command.frameS3Path()))).thenAnswer(invocation -> {
            uploads.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });

        final var first = useCase.execute(command).join();
        final var second = useCase.execute(command).join();

        assertThat(second.videoId()).isEqualTo(first.videoId());
        assertThat(second.frameS3Path()).isEqualTo(first.frameS3Path());
        assertThat(downloads.get()).isEqualTo(1);
        assertThat(extractions.get()).isEqualTo(1);
        assertThat(uploads.get()).isEqualTo(1);
    }

    @Test
    void should_download_again_when_previous_task_finished_and_video_was_cleaned() {
        var downloads = new AtomicInteger();
        final var command1 = command(102L, "s3://bucket/frames/frame.png");
        final var command2 = command(103L, "s3://bucket/frames/frame2.png");

        stubJobRepositoryForNormalFlow(command1);
        stubJobRepositoryForNormalFlow(command2);
        when(jobStateRepository.incrementVideoRef(anyString())).thenReturn(CompletableFuture.completedFuture(1L));
        when(jobStateRepository.decrementVideoRef(anyString())).thenReturn(CompletableFuture.completedFuture(0L));

        when(fileRepository.download(any(), any(Path.class))).thenAnswer(invocation -> {
            downloads.incrementAndGet();
            Path localVideo = invocation.getArgument(1);
            Files.createDirectories(localVideo.getParent());
            Files.writeString(localVideo, "video-bytes");
            return CompletableFuture.completedFuture(null);
        });
        when(frameExtractionPort.extractFrame(any(Path.class), any(Path.class), any(Double.class))).thenAnswer(invocation -> {
            Path localFrame = invocation.getArgument(1);
            Files.writeString(localFrame, "frame-bytes");
            return CompletableFuture.completedFuture(Duration.ofMillis(1));
        });
        when(fileRepository.upload(any(Path.class), any())).thenReturn(CompletableFuture.completedFuture(null));

        useCase.execute(command1).join();
        useCase.execute(command2).join();

        assertThat(downloads.get()).isEqualTo(2);
    }

    @Test
    void should_download_once_for_concurrent_tasks_using_same_video() throws Exception {
        var downloads = new AtomicInteger();
        final var started = new CountDownLatch(2);
        final var release = new CountDownLatch(1);
        var refs = new AtomicInteger();

        final var command1 = command(201L, "s3://bucket/frames/frame-1.png");
        final var command2 = command(202L, "s3://bucket/frames/frame-2.png");

        stubJobRepositoryForNormalFlow(command1);
        stubJobRepositoryForNormalFlow(command2);
        when(jobStateRepository.incrementVideoRef(eq(command1.videoS3Path())))
            .thenAnswer(invocation -> CompletableFuture.completedFuture((long) refs.incrementAndGet()));
        when(jobStateRepository.decrementVideoRef(eq(command1.videoS3Path())))
            .thenAnswer(invocation -> CompletableFuture.completedFuture((long) Math.max(refs.decrementAndGet(), 0)));

        when(fileRepository.download(eq(command1.videoS3Path()), any(Path.class))).thenAnswer(invocation -> {
            downloads.incrementAndGet();
            Path localVideo = invocation.getArgument(1);
            Files.createDirectories(localVideo.getParent());
            Files.writeString(localVideo, "video-bytes");
            return CompletableFuture.completedFuture(null);
        });
        when(frameExtractionPort.extractFrame(any(Path.class), any(Path.class), any(Double.class))).thenAnswer(invocation -> {
            started.countDown();
            release.await(2, TimeUnit.SECONDS);
            Path localFrame = invocation.getArgument(1);
            Files.createDirectories(localFrame.getParent());
            Files.writeString(localFrame, "frame-bytes");
            return CompletableFuture.completedFuture(Duration.ofMillis(5));
        });
        when(fileRepository.upload(any(Path.class), any())).thenReturn(CompletableFuture.completedFuture(null));

        var executor = Executors.newFixedThreadPool(2);
        try {
            Future<ExtractFrameResult> future1 = executor.submit(() -> useCase.execute(command1).get(2, TimeUnit.SECONDS));
            Future<ExtractFrameResult> future2 = executor.submit(() -> useCase.execute(command2).get(2, TimeUnit.SECONDS));

            started.await(2, TimeUnit.SECONDS);
            release.countDown();

            future1.get(2, TimeUnit.SECONDS);
            future2.get(2, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertThat(downloads.get()).isEqualTo(1);
        verify(fileRepository, times(1)).download(eq(command1.videoS3Path()), any(Path.class));
    }

    private void stubJobRepositoryForNormalFlow(ExtractFrameCommand command) {
        String jobId = command.videoId() + "|" + command.frameS3Path() + "|" + command.second();
        when(jobStateRepository.findJob(eq(jobId))).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(jobStateRepository.markRunning(eq(jobId), eq(command))).thenReturn(CompletableFuture.completedFuture(null));
        when(jobStateRepository.markSuccess(eq(jobId), any(ExtractFrameResult.class))).thenReturn(CompletableFuture.completedFuture(null));
    }

    private ExtractFrameCommand command(long videoId, String frameS3Path) {
        return new ExtractFrameCommand(videoId, "s3://bucket/videos/video.mp4", frameS3Path, 1.0);
    }

}
