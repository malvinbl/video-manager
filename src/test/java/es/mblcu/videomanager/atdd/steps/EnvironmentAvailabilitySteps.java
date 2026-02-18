package es.mblcu.videomanager.atdd.steps;

import es.mblcu.videomanager.atdd.support.DockerComposeSupport;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;
import io.cucumber.java.en.Then;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class EnvironmentAvailabilitySteps {

    private final DockerComposeSupport docker = new DockerComposeSupport(Path.of(".").toAbsolutePath().normalize());
    private List<String> runningServices;

    @Given("que el stack docker de video-manager esta iniciado")
    public void stackIniciado() {
        runningServices = docker.runningServices();
        assertThat(runningServices).isNotEmpty();
    }

    @When("consulto los servicios docker en ejecucion")
    public void consultoServiciosEnEjecucion() {
        runningServices = docker.runningServices();
    }

    @Then("los servicios {string} estan en estado running")
    public void serviciosEsperadosEnRunning(String expectedCsv) {
        List<String> expected = Arrays.stream(expectedCsv.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .toList();

        for (String service : expected) {
            assertThat(runningServices)
                .as("Service not found in running state: " + service + ". running=" + runningServices)
                .contains(service);
        }
    }

}
