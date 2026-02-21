package es.mblcu.videomanager.application.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class LocalWorkspaceService {

    private final Path videosDir;
    private final Path framesDir;
    private final Path transcodedDir;

    public LocalWorkspaceService(Path baseDir) {
        this.videosDir = baseDir.resolve("videos");
        this.framesDir = baseDir.resolve("frames");
        this.transcodedDir = baseDir.resolve("transcoded");
    }

    public Path resolveLocalVideoPath(String s3Path) {
        ensureDirectory(videosDir);
        String name = fileNamePart(s3Path);
        return videosDir.resolve(hash(s3Path) + "-" + name);
    }

    public Path resolveLocalFramePath(String s3Path) {
        ensureDirectory(framesDir);
        String name = fileNamePart(s3Path);
        return framesDir.resolve(hash(s3Path) + "-" + name);
    }

    public Path resolveLocalTranscodedPath(String s3Path) {
        ensureDirectory(transcodedDir);
        String name = fileNamePart(s3Path);
        return transcodedDir.resolve(hash(s3Path) + "-" + name);
    }

    public boolean exists(Path file) {
        return Files.exists(file);
    }

    public void deleteQuietly(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException ignored) {
            // Best effort cleanup.
        }
    }

    public void ensureParentDirectory(Path file) {
        final var parent = file.getParent();
        if (parent != null) {
            ensureDirectory(parent);
        }
    }

    private void ensureDirectory(Path directory) {
        try {
            Files.createDirectories(directory);
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot create directory: " + directory, ex);
        }
    }

    private String fileNamePart(String s3Path) {
        int idx = s3Path.lastIndexOf('/');
        if (idx == -1 || idx == s3Path.length() - 1) {
            return "media.bin";
        }
        return s3Path.substring(idx + 1);
    }

    private String hash(String value) {
        try {
            final var digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(value.getBytes());
            final var sb = new StringBuilder();
            for (int i = 0; i < 8 && i < hashBytes.length; i++) {
                sb.append(String.format("%02x", hashBytes[i]));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 not available", ex);
        }
    }

}
