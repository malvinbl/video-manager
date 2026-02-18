package es.mblcu.videomanager.atdd.hooks;

import es.mblcu.videomanager.atdd.support.DockerComposeSupport;
import io.cucumber.java.AfterAll;
import io.cucumber.java.BeforeAll;

import java.nio.file.Path;

public class DockerHooks {

    private static final DockerComposeSupport DOCKER = new DockerComposeSupport(Path.of(".").toAbsolutePath().normalize());

    @BeforeAll
    public static void startStack() {
        DOCKER.up();
    }

    @AfterAll
    public static void stopStack() {
        DOCKER.down();
    }

}
