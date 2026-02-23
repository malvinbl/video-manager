import groovy.json.JsonOutput
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import java.time.Instant
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

final AtomicLong counter = props.get("vm.counter") as AtomicLong ?: new AtomicLong(0)
props.put("vm.counter", counter)

String flow = vars.get("flow")
String bootstrapServers = props.getProperty("bootstrapServers", "localhost:9092")
String videoS3Path = props.getProperty("videoS3Path", "s3://bucket/videos/sample.mp4")

String topic
Map payload
long id = (System.currentTimeMillis() * 1000L) + counter.incrementAndGet()

if ("extract".equals(flow)) {
    topic = props.getProperty("extractRequestTopic", "fe_sample_frame_request__norm")
    String framePath = "s3://bucket/frames/stress/frame-${id}.png"
    payload = [
        videoId    : id,
        videoS3Path: videoS3Path,
        frameS3Path: framePath,
        second     : 1.0d
    ]
} else if ("transcode".equals(flow)) {
    topic = props.getProperty("transcodeRequestTopic", "fe_transcode_request__norm")
    String outputPrefix = "s3://bucket/transcoded/stress/${id}"
    List<Map<String, Integer>> allowed = [
        [width: 1920, height: 1080],
        [width: 1280, height: 720],
        [width: 854, height: 480],
        [width: 640, height: 360]
    ]
    Map<String, Integer> chosen = allowed[(int) (id % allowed.size())]
    payload = [
        videoId       : id,
        videoS3Path   : videoS3Path,
        outputS3Prefix: outputPrefix,
        width         : chosen.width,
        height        : chosen.height
    ]
} else {
    throw new IllegalArgumentException("Unsupported flow: " + flow)
}

Properties kafkaProps = new Properties()
kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
kafkaProps.put(ProducerConfig.ACKS_CONFIG, "all")
kafkaProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000")
kafkaProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
kafkaProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000")
kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, "0")

KafkaProducer<String, String> producer = null
try {
    producer = new KafkaProducer<>(kafkaProps)
    String value = JsonOutput.toJson(payload)
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(id), value)
    producer.send(record).get(5, TimeUnit.SECONDS)

    SampleResult.setSuccessful(true)
    SampleResult.setResponseCodeOK()
    SampleResult.setResponseMessage("Published to " + topic + " id=" + id)
    SampleResult.setResponseData(value, "UTF-8")
} catch (Throwable t) {
    SampleResult.setSuccessful(false)
    SampleResult.setResponseCode("500")
    SampleResult.setResponseMessage(t.getMessage())
    log.error("Kafka publish failed flow={} time={}", flow, Instant.now(), t)
} finally {
    if (producer != null) {
        producer.close()
    }
}
