package es.mblcu.videomanager.application.usecase;

import es.mblcu.videomanager.domain.transcode.TranscodeProfile;

import java.util.List;
import java.util.Map;

public class TranscodeProfileCatalog {

    private final Map<String, List<TranscodeProfile>> profilesByInputDimension;

    public TranscodeProfileCatalog(Map<String, List<TranscodeProfile>> profilesByInputDimension) {
        this.profilesByInputDimension = profilesByInputDimension;
    }

    public List<TranscodeProfile> profilesFor(int width, int height) {
        return profilesByInputDimension.get(width + "x" + height);
    }

    public boolean supports(int width, int height) {
        return profilesByInputDimension.containsKey(width + "x" + height);
    }

}
