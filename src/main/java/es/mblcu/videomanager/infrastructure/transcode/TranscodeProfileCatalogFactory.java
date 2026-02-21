package es.mblcu.videomanager.infrastructure.transcode;

import es.mblcu.videomanager.application.service.TranscodeProfileCatalogService;
import es.mblcu.videomanager.infrastructure.config.AppProperties;
import es.mblcu.videomanager.domain.transcode.TranscodeProfile;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TranscodeProfileCatalogFactory {

    public static TranscodeProfileCatalogService fromProperties() {
        final var properties = AppProperties.load();
        String allowedRaw = properties.get(
            "TRANSCODE_ALLOWED_DIMENSIONS",
            "transcode.allowed-dimensions",
            "1920x1080,1280x720,854x480,640x360"
        );

        Map<String, List<TranscodeProfile>> profilesByInput = new LinkedHashMap<>();
        List<String> allowed = Arrays.stream(allowedRaw.split(","))
            .map(String::trim)
            .filter(s -> !s.isBlank())
            .toList();

        for (String dimension : allowed) {
            String key = "transcode.profiles." + dimension;
            String env = "TRANSCODE_PROFILES_" + dimension.replace("x", "_");
            String defaultProfiles = defaultProfilesForDimension(dimension);
            String profilesRaw = properties.get(env, key, defaultProfiles);
            profilesByInput.put(dimension, parseProfiles(dimension, profilesRaw));
        }

        return new TranscodeProfileCatalogService(profilesByInput);
    }

    private static List<TranscodeProfile> parseProfiles(String inputDimension, String raw) {
        return Arrays.stream(raw.split(","))
            .map(String::trim)
            .filter(s -> !s.isBlank())
            .map(profileRaw -> parseProfile(inputDimension, profileRaw))
            .toList();
    }

    private static TranscodeProfile parseProfile(String inputDimension, String raw) {
        String[] parts = raw.split("@");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid profile entry for " + inputDimension + ": " + raw);
        }

        String[] dim = parts[0].split("x");
        if (dim.length != 2) {
            throw new IllegalArgumentException("Invalid output dimension for " + inputDimension + ": " + raw);
        }

        int width = Integer.parseInt(dim[0]);
        int height = Integer.parseInt(dim[1]);
        int bitrate = Integer.parseInt(parts[1].replace("k", ""));
        String name = width + "x" + height;

        return new TranscodeProfile(name, width, height, bitrate);
    }

    private static String defaultProfilesForDimension(String dimension) {
        return switch (dimension) {
            case "1920x1080" -> "1280x720@2800k,854x480@1400k,640x360@800k";
            case "1280x720" -> "854x480@1400k,640x360@800k,426x240@400k";
            case "854x480" -> "640x360@800k,426x240@400k,256x144@200k";
            case "640x360" -> "426x240@400k,256x144@200k,192x108@120k";
            default -> "426x240@400k,256x144@200k,192x108@120k";
        };
    }

}
