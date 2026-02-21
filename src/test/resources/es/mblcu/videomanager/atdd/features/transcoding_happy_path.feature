Feature: Video transcoding
  As a platform consumer
  I want to transcode a source S3 video into multiple profiles
  So that I get transcoded outputs and status via Kafka

  Scenario: Happy path video transcoding
    Given que el stack docker de video-manager esta iniciado
    And un video de origen disponible en S3 para transcodear
    When publico una request de transcoding en Kafka
    Then se recibe respuesta Kafka success y los videos transcodeados quedan en S3
    And se elimina el fichero origen local de transcoding cuando corresponde
