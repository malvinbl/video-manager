Feature: Frame extraction
  As a platform consumer
  I want to extract a frame from an S3 video
  So that I get the result image and status via Kafka

  Scenario: Happy path frame extraction
    Given que el stack docker de video-manager esta iniciado
    And un video de origen disponible en S3 para la extraccion
    When publico una request de extraccion de frame en Kafka
    Then se recibe respuesta Kafka success y el frame queda generado en S3
    And se elimina el fichero origen descargado en local cuando corresponde
