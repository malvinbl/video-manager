package es.mblcu.videomanager.atdd.steps;

import com.fasterxml.jackson.databind.ObjectMapper;
import es.mblcu.videomanager.atdd.support.DockerComposeSupport;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.minio.MinioClient;
import io.minio.StatObjectArgs;
import io.minio.UploadObjectArgs;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

public class FrameExtractionHappyPathSteps {

    private static final String REQUEST_TOPIC = "fe_sample_frame_request__norm";
    private static final String RESPONSE_TOPIC = "fe_sample_frame_done__norm";
    private static final String APP_GROUP_ID = "videomanager-extract-frame";
    private static final String S3_BUCKET = "bucket";
    private static final String KAFKA_BOOTSTRAP = "localhost:19092";

    private final DockerComposeSupport docker = new DockerComposeSupport(Path.of(".").toAbsolutePath().normalize());
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final MinioClient minioClient = MinioClient.builder()
        .endpoint("http://localhost:9000")
        .credentials("minio-root-user", "minio-root-password")
        .build();

    private long videoId;
    private String videoS3Path;
    private String frameS3Path;

    @Given("un video de origen disponible en S3 para la extraccion")
    public void sourceVideoAvailableInS3() throws Exception {
        videoId = System.currentTimeMillis();
        String videoObject = "videos/atdd-input-" + videoId + ".mp4";
        String frameObject = "frames/atdd-frame-" + videoId + ".png";
        videoS3Path = "s3://" + S3_BUCKET + "/" + videoObject;
        frameS3Path = "s3://" + S3_BUCKET + "/" + frameObject;

        docker.execInService(
            "app",
            "sh",
            "-lc",
            "ffmpeg -hide_banner -loglevel error -y -f lavfi -i testsrc=size=320x240:rate=25 -t 2 /tmp/atdd-input.mp4"
        );

        final var localVideo = Files.createTempFile("atdd-input-", ".mp4");
        docker.copyFromService("app", "/tmp/atdd-input.mp4", localVideo);

        minioClient.uploadObject(
            UploadObjectArgs.builder()
                .bucket(S3_BUCKET)
                .object(videoObject)
                .filename(localVideo.toString())
                .build()
        );

        assertThat(objectExists(videoObject))
            .as("Source video was not uploaded to S3")
            .isTrue();

        Files.deleteIfExists(localVideo);
    }

    @When("publico una request de extraccion de frame en Kafka")
    public void publishFrameExtractionRequestToKafka() throws Exception {
        ensureTopics();
        waitUntilAppConsumerReady();

        String payload = objectMapper.writeValueAsString(Map.of(
            "videoId", videoId,
            "videoS3Path", videoS3Path,
            "frameS3Path", frameS3Path,
            "second", 1.0
        ));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties())) {
            producer.send(new ProducerRecord<>(REQUEST_TOPIC, String.valueOf(videoId), payload)).get();
            producer.flush();
        }
    }

    @Then("se recibe respuesta Kafka success y el frame queda generado en S3")
    public void kafkaSuccessResponseReceivedAndFrameGeneratedInS3() {
        String frameObject = frameS3Path.replace("s3://" + S3_BUCKET + "/", "");

        Map<String, Object> response = waitForMatchingResponse();
        assertThat(response.get("status"))
            .as("Kafka response status is not success")
            .isEqualTo("success");
        assertThat(response.get("frameS3Path"))
            .as("Kafka response frame path mismatch")
            .isEqualTo(frameS3Path);

        await()
            .atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                assertThat(objectExists(frameObject))
                    .as("Frame not found in S3 yet")
                    .isTrue();
            });
    }

    @And("se elimina el fichero origen descargado en local cuando corresponde")
    public void sourceLocalFileIsDeletedWhenNoLongerReferenced() {
        final String localVideoPath = buildExpectedLocalVideoPath(videoS3Path);

        await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> assertThat(localFileExistsInApp(localVideoPath))
                .as("Source local video still exists: " + localVideoPath)
                .isFalse());
    }

    private Map<String, Object> waitForMatchingResponse() {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties())) {
            consumer.subscribe(List.of(RESPONSE_TOPIC));

            while (System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : records) {
                    Map<String, Object> payload = objectMapper.readValue(record.value(), Map.class);
                    Object responseVideoId = payload.get("videoId");
                    if (responseVideoId == null || Long.parseLong(responseVideoId.toString()) != videoId) {
                        continue;
                    }
                    return payload;
                }
            }
        } catch (Exception ex) {
            throw new IllegalStateException("Cannot read Kafka response topic", ex);
        }

        throw new AssertionError("No matching Kafka response found yet");
    }

    private boolean objectExists(String object) {
        try {
            minioClient.statObject(
                StatObjectArgs.builder()
                    .bucket(S3_BUCKET)
                    .object(object)
                    .build()
            );
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    private boolean localFileExistsInApp(String absolutePath) {
        String output = docker.execInServiceAndGetOutput(
            "app",
            "sh",
            "-lc",
            "if [ -f '" + absolutePath + "' ]; then echo exists; else echo missing; fi"
        );
        return output != null && output.trim().contains("exists");
    }

    private String buildExpectedLocalVideoPath(String s3Path) {
        String fileName = fileNamePart(s3Path);
        String hash = shortSha256(s3Path);
        return "/tmp/.videomanager-work/videos/" + hash + "-" + fileName;
    }

    private String fileNamePart(String s3Path) {
        int idx = s3Path.lastIndexOf('/');
        if (idx == -1 || idx == s3Path.length() - 1) {
            return "media.bin";
        }
        return s3Path.substring(idx + 1);
    }

    private String shortSha256(String value) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(value.getBytes());
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 8 && i < hash.length; i++) {
                sb.append(String.format("%02x", hash[i]));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 not available", ex);
        }
    }

    private Properties producerProperties() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

    private Properties consumerProperties() {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "atdd-frame-response-" + videoId + "-" + System.nanoTime());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }

    private void ensureTopics() {
        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);

        try (AdminClient admin = AdminClient.create(props)) {
            var request = new NewTopic(REQUEST_TOPIC, 1, (short) 1);
            var response = new NewTopic(RESPONSE_TOPIC, 1, (short) 1);
            admin.createTopics(List.of(request, response)).all().get();
        } catch (ExecutionException ex) {
            if (!(ex.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException("Cannot create Kafka topics for ATDD", ex);
            }
        } catch (Exception ex) {
            throw new IllegalStateException("Cannot create Kafka topics for ATDD", ex);
        }
    }

    private void waitUntilAppConsumerReady() {
        await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> assertThat(isAppConsumerGroupReady())
                .as("App Kafka consumer group is not ready yet")
                .isTrue());
    }

    private boolean isAppConsumerGroupReady() {
        try {
            String output = docker.execInServiceAndGetOutput(
                "kafka",
                "sh",
                "-lc",
                "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group " + APP_GROUP_ID + " --describe"
            );
            if (StringUtils.isEmpty(output)) {
                return false;
            }

            return output.lines()
                .anyMatch(line -> line.contains(REQUEST_TOPIC) && line.contains("consumer-"));
        } catch (Exception ex) {
            return false;
        }
    }

}
