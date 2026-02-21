package es.mblcu.videomanager.application.service;

import es.mblcu.videomanager.domain.transcode.TranscodeProfile;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class TranscodeProfileCatalogService {

    private final Map<String, List<TranscodeProfile>> profilesByInputDimension;

    public List<TranscodeProfile> profilesFor(int width, int height) {
        return profilesByInputDimension.get(width + "x" + height);
    }

    public boolean supports(int width, int height) {
        return profilesByInputDimension.containsKey(width + "x" + height);
    }

}
