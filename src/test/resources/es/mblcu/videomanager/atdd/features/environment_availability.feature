# language: es
Característica: Entorno docker local
  Como equipo de desarrollo
  Quiero validar el stack local del servicio
  Para ejecutar pruebas de aceptación en un entorno realista

  Escenario: Servicios base disponibles
    Dado que el stack docker de video-manager esta iniciado
    Cuando consulto los servicios docker en ejecucion
    Entonces los servicios "kafka,redis,minio,app" estan en estado running
