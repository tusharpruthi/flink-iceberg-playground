package com.hevo.icebergplayground.config;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Loads an {@link AppConfig} from an {@code application-<environment>.yml} classpath resource
 * (or an absolute file path override), substituting {@code ${ENV_VAR}} / {@code ${ENV_VAR:default}}
 * placeholders from the process environment. Keeping secrets out of the YAML files and in
 * environment variables is what lets the same file be reused across local/prod without edits.
 */
public final class ConfigLoader {

    private static final Pattern ENV_PLACEHOLDER = Pattern.compile("\\$\\{([A-Za-z0-9_]+)(:([^}]*))?}");

    private ConfigLoader() {
    }

    public static AppConfig load(String environment) {
        String resourceName = "application-" + environment + ".yml";
        String rawYaml = readResource(resourceName);
        String resolvedYaml = substituteEnvPlaceholders(rawYaml, System::getenv);
        Yaml yaml = new Yaml(new Constructor(AppConfig.class, new LoaderOptions()));
        return yaml.load(resolvedYaml);
    }

    public static AppConfig loadFromFile(Path path) {
        try {
            String rawYaml = Files.readString(path, StandardCharsets.UTF_8);
            String resolvedYaml = substituteEnvPlaceholders(rawYaml, System::getenv);
            Yaml yaml = new Yaml(new Constructor(AppConfig.class, new LoaderOptions()));
            return yaml.load(resolvedYaml);
        } catch (IOException e) {
            throw new ConfigLoadException("Failed to read config file: " + path, e);
        }
    }

    static String substituteEnvPlaceholders(String yaml, Function<String, String> envLookup) {
        Matcher matcher = ENV_PLACEHOLDER.matcher(yaml);
        StringBuilder result = new StringBuilder();
        int lastEnd = 0;
        while (matcher.find()) {
            result.append(yaml, lastEnd, matcher.start());
            String variableName = matcher.group(1);
            String defaultValue = matcher.group(3);
            String value = envLookup.apply(variableName);
            if (value == null) {
                value = defaultValue;
            }
            if (value == null) {
                throw new ConfigLoadException(
                        "Missing environment variable '" + variableName + "' referenced in config and no default provided");
            }
            result.append(value);
            lastEnd = matcher.end();
        }
        result.append(yaml.substring(lastEnd));
        return result.toString();
    }

    private static String readResource(String resourceName) {
        try (InputStream inputStream = ConfigLoader.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (inputStream == null) {
                throw new ConfigLoadException("Config resource not found on classpath: " + resourceName);
            }
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new ConfigLoadException("Failed to read config resource: " + resourceName, e);
        }
    }

    public static class ConfigLoadException extends RuntimeException {
        public ConfigLoadException(String message) {
            super(message);
        }

        public ConfigLoadException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
