package es.mblcu.videomanager.infrastructure.transcode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TranscodeProfileCatalogFactoryTest {

    @AfterEach
    void clear_properties() {
        System.clearProperty("transcode.allowed-dimensions");
        System.clearProperty("transcode.profiles.1920x1080");
        System.clearProperty("transcode.profiles.1280x720");
        System.clearProperty("transcode.profiles.854x480");
        System.clearProperty("transcode.profiles.640x360");
        System.clearProperty("transcode.profiles.1024x576");
    }

    @Test
    void should_load_configured_defaults() {
        final var catalog = TranscodeProfileCatalogFactory.fromProperties();

        assertThat(catalog.supports(1280, 720)).isTrue();
        assertThat(catalog.profilesFor(1280, 720)).hasSize(3);
        assertThat(catalog.profilesFor(1280, 720).get(0).name()).isEqualTo("854x480");
        assertThat(catalog.profilesFor(1280, 720).get(0).videoBitrateKbps()).isEqualTo(1400);
    }

    @Test
    void should_override_profiles_from_system_properties() {
        System.setProperty("transcode.allowed-dimensions", "1024x576");
        System.setProperty("transcode.profiles.1024x576", "640x360@900k,426x240@500k,256x144@250k");

        final var catalog = TranscodeProfileCatalogFactory.fromProperties();

        assertThat(catalog.supports(1024, 576)).isTrue();
        assertThat(catalog.supports(1280, 720)).isFalse();
        assertThat(catalog.profilesFor(1024, 576)).hasSize(3);
        assertThat(catalog.profilesFor(1024, 576).get(0).name()).isEqualTo("640x360");
        assertThat(catalog.profilesFor(1024, 576).get(0).videoBitrateKbps()).isEqualTo(900);
    }

    @Test
    void should_fail_when_profile_definition_is_invalid() {
        System.setProperty("transcode.allowed-dimensions", "1024x576");
        System.setProperty("transcode.profiles.1024x576", "invalid-profile");

        assertThatThrownBy(TranscodeProfileCatalogFactory::fromProperties)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid profile entry");
    }

}
