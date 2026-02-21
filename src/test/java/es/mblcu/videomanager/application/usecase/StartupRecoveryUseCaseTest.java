package es.mblcu.videomanager.application.usecase;

import es.mblcu.videomanager.domain.frame.ExtractFrameCommand;
import es.mblcu.videomanager.domain.jobs.JobState;
import es.mblcu.videomanager.domain.jobs.JobStateRepository;
import es.mblcu.videomanager.domain.jobs.vo.JobStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StartupRecoveryUseCaseTest {

    @Mock
    private JobStateRepository repository;

    private StartupRecoveryUseCase useCase;

    @BeforeEach
    void setUp() {
        useCase = new StartupRecoveryUseCase(repository);
    }

    @Test
    void should_return_zero_when_no_running_jobs() {
        when(repository.findJobsByStatus(JobStatus.RUNNING)).thenReturn(CompletableFuture.completedFuture(List.of()));

        int recovered = useCase.recoverRunningJobs().join();

        assertThat(recovered).isZero();
        verify(repository, never()).markError(any(), any(), any());
    }

    @Test
    void should_mark_running_jobs_as_error_on_startup() {
        final var running = new JobState(
            "100|s3://bucket/frames/frame.png|2.5",
            100L,
            "s3://bucket/videos/video.mp4",
            "s3://bucket/frames/frame.png",
            JobStatus.RUNNING,
            null,
            null
        );

        when(repository.findJobsByStatus(JobStatus.RUNNING)).thenReturn(CompletableFuture.completedFuture(List.of(running)));
        when(repository.markError(eq(running.jobId()), any(ExtractFrameCommand.class), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        int recovered = useCase.recoverRunningJobs().join();

        assertThat(recovered).isEqualTo(1);

        ArgumentCaptor<ExtractFrameCommand> commandCaptor = ArgumentCaptor.forClass(ExtractFrameCommand.class);
        verify(repository).markError(eq(running.jobId()), commandCaptor.capture(), eq("Recovered at startup after previous unclean shutdown"));

        final var command = commandCaptor.getValue();

        assertThat(command.videoId()).isEqualTo(100L);
        assertThat(command.videoS3Path()).isEqualTo("s3://bucket/videos/video.mp4");
        assertThat(command.frameS3Path()).isEqualTo("s3://bucket/frames/frame.png");
        assertThat(command.second()).isEqualTo(2.5d);
    }

}
