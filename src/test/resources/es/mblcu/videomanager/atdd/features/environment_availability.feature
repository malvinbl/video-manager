Feature: Local docker environment
  As a development team
  I want to validate the local service stack
  So that acceptance tests run in a realistic setup

  Scenario: Base services are available
    Given que el stack docker de video-manager esta iniciado
    When consulto los servicios docker en ejecucion
    Then los servicios "kafka,redis,minio,app" estan en estado running
