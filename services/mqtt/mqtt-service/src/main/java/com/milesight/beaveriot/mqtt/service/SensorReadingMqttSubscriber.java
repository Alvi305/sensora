package com.milesight.beaveriot.mqtt.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.milesight.beaveriot.context.mqtt.enums.MqttTopicChannel;
import com.milesight.beaveriot.context.mqtt.listener.MqttMessageListener;
import com.milesight.beaveriot.context.mqtt.model.MqttMessage;
import com.milesight.beaveriot.mqtt.model.SensorReading;
import com.milesight.beaveriot.mqtt.repository.SensorReadingsRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Component
@RequiredArgsConstructor
public class SensorReadingMqttSubscriber {

    private final MqttPubSubService mqtt;
    private final SensorReadingsRepository repository;
    private final ObjectMapper mapper = new ObjectMapper();

    private static final Pattern KV_COLON = Pattern.compile("^\\s*([^:]+?)\\s*[:：]\\s*(.+)\\s*$");
    private static final Pattern JSON_BLOCK_PATTERN = Pattern.compile("\\{[\\s\\S]*}$");

    // Extract GwEUI from topic
    private static final Pattern TOPIC_GWEUI = Pattern.compile(".*/milesight-gateway/([0-9a-fA-F:\\-]+?)/uplink$");

    // Alias sets for resilient parsing
    private static final String[] DEV_EUI_ALIASES =
    {
            "device eui", "device eui/group name", "devEUI", "dev_eui", "devEui", "deviceEui", "Device EUI/Group Name", "identifier"
    };

    private static final String[] GW_EUI_ALIASES =
    {
            "gw eui", "gateway eui", "gw_eui", "gwEui", "GwEUI", "gatewayEui", "mac"
    };

    private static final String[] APP_NAME_ALIASES = {
            "applicationName", "appName"
    };

    private static final String[] DEVICE_NAME_ALIASES = {
            "deviceName", "device_name"
    };


    private static final String[] FCNT_ALIASES = { "fCnt" };

    @PostConstruct
    public void subscribeAll() {
        // Subscribe to all users and all subtopics on the DEFAULT channel
        mqtt.subscribe(MqttTopicChannel.DEFAULT,"+","#",listener(),true);
    }

    private MqttMessageListener listener() {
        return msg -> {
            try {
                processMessage(msg);
            } catch (Exception e)  {
                log.error("Failed to handle MQTT message topic={}",msg.getFullTopicName(), e);
            }
        };
    }

    private void processMessage(MqttMessage msg) {
        final String payload = new String(msg.getPayload(), StandardCharsets.UTF_8).trim();

        log.warn("MQTT incoming: topic={} tenant={} username={} msgpayload={} payload={} ",
                msg.getFullTopicName(), msg.getTenantId(), msg.getUsername(), msg.getPayload(),
                abbreviate(payload, 4000));

//        Map<String, String> kvMap = parseKvLines(payload);
          JsonNode node = parseJson(payload);

        SensorReading reading = new SensorReading();
        reading.setTenantId(msg.getTenantId());

        // extract GwEUI from rxinfo.mac or from topic path
        if (isBlank(reading.getGwEui())) {
            reading.setGwEui(extractGatewayEuiFromRxInfo(node));
        }

        if (isBlank(reading.getGwEui())) {
            reading.setGwEui(extractGatewayEuiFromTopic(msg.getFullTopicName()));
        }

//        populateFromKv(reading, kvMap);
        populateFromJson(reading, node);
        populateTelemetry(reading, node);
        normalizeIdentifiers(reading);

        validateAndPersist(reading, msg);
    }

    // Parsing: KV section for .text format data
//    private Map<String, String> parseKvLines(String text) {
//        Map<String, String> map = new LinkedHashMap<>();
//        String[] lines = text.split("\\r?\\n");
//
//        for (String raw : lines) {
//            String line = raw == null ? "" : raw.trim();
//            if (line.isEmpty()) continue;
//            if (line.startsWith("{")) break; // stop at JSON block
//
//            Matcher m = KV_COLON.matcher(line);
//            if (m.matches()) {
//                map.put(normalizeKey(m.group(1)), m.group(2).trim());
//                continue;
//            }
//
//            String[] knownKeys = {
//                    "Dev Addr/Multicast Addr", "Dev Addr", "Multicast Addr",
//                    "GwEUI", "AppEUI", "Device EUI/Group Name", "Device EUI","devEUI",
//                    "Fcnt", "FCnt", "Port", "deviceName", "applicationName"
//            };
//            for (String k : knownKeys) {
//                if (startsWithIgnoreCase(line, k)) {
//                    map.put(normalizeKey(k), line.substring(k.length()).trim());
//                    break;
//                }
//            }
//        }
//
//    //log.warn("KV parsed entries: {}", map);
//
//        return map;
//    }

    // Parsing: JSON section
    private JsonNode parseJson(String text) {
        try {
            if (text.startsWith("{") && text.endsWith("}")) {
                return mapper.readTree(text);
            }
        } catch (JsonProcessingException e) {
            log.warn("Failed to parse full payload as JSON", e);
        }

        Matcher m = JSON_BLOCK_PATTERN.matcher(text);
        if (m.find()) {
            String matched = m.group(0);
            try {
                return mapper.readTree(matched);
            } catch (JsonProcessingException e) {
                log.warn("Failed to parse trailing JSON block", e);
            }
        }
        return null;
    }

    // Populate identifiers and counters from KV
    private void populateFromKv(SensorReading reading, Map<String, String> kv) {
        if (kv == null || kv.isEmpty()) return;

        reading.setDevEui(firstNonBlank(
                getFromMap(kv, DEV_EUI_ALIASES)
        ));
        reading.setGwEui(firstNonBlank(
                getFromMap(kv, GW_EUI_ALIASES)
        ));

        if (isBlank(reading.getDeviceName())) {
            reading.setDeviceName(firstNonBlank(getFromMap(kv, DEVICE_NAME_ALIASES)));
        }
        if (isBlank(reading.getAppName())) {
            reading.setAppName(firstNonBlank(getFromMap(kv, APP_NAME_ALIASES)));
        }

        String fcntStr = firstNonBlank(getFromMap(kv, FCNT_ALIASES));
        reading.setFcnt(parseLongSafe(fcntStr));

//        log.warn("in method populateFromKv: {}", formatReading(reading));
    }

    // Populate identifiers and counters from JSON
    private void populateFromJson(SensorReading reading, JsonNode root) {
        if (root == null) return;

        if (isBlank(reading.getDevEui())) {
            reading.setDevEui(firstNonBlank(
                getFirstText(root, DEV_EUI_ALIASES)
            ));
        }
        if (isBlank(reading.getGwEui())) {
            reading.setGwEui(firstNonBlank(
                getFirstText(root, GW_EUI_ALIASES)
            ));
        }

        if (reading.getFcnt() == null) {
            Long fcnt = firstNonNull(
                getFirstLong(root, FCNT_ALIASES)
            );
            reading.setFcnt(fcnt);
        }

        if (isBlank(reading.getDeviceName())) {
            reading.setDeviceName(firstNonBlank(getFirstText(root, DEVICE_NAME_ALIASES)));
        }
        if (isBlank(reading.getAppName())) {
            reading.setAppName(firstNonBlank(getFirstText(root, APP_NAME_ALIASES)));
        }

//        log.warn("in method populateFromJson: {}", formatReading(reading));
    }

    // Populate telemetry values
    private void populateTelemetry(SensorReading reading, JsonNode root) {
        if (root == null) {
            log.warn("temperature, humidity payload MISSING");
            return;
        }

        if (reading.getTemperature() == null) {
            reading.setTemperature(getDouble(root, "temperature"));
        }
        if (reading.getHumidity() == null) {
            reading.setHumidity(getDouble(root, "humidity"));
        }
        if (reading.getBattery() == null) {
            reading.setBattery(getDouble(root, "battery"));
        }

        if (root.has("payload")) {
            JsonNode payload = root.get("payload");
            if (reading.getTemperature() == null) {
                reading.setTemperature(getDouble(payload, "temperature"));
            }
            if (reading.getHumidity() == null) {
                reading.setHumidity(getDouble(payload, "humidity"));
            }
            if (reading.getBattery() == null) {
                reading.setBattery(getDouble(payload, "battery"));
            }
        }

//        log.warn("in method populateTelemetry: {}", formatReading(reading));
    }

    // Normalize identifiers
    private void normalizeIdentifiers(SensorReading reading) {
        reading.setDevEui(normalizeEui(reading.getDevEui()));
        reading.setGwEui(normalizeEui(reading.getGwEui()));
    }

    // Persist with validation
    private void validateAndPersist(SensorReading reading, MqttMessage msg) {
        if (isBlank(reading.getTenantId()) || isBlank(reading.getDevEui()) || reading.getTemperature() == null || reading.getHumidity() == null) {
            log.warn("Missing tenantId/devEui/temperature/humidity; skipping persist. tenantId={} devEui={} topic={} temp={} humidity={}",
                    reading.getTenantId(), reading.getDevEui(), msg.getFullTopicName(), reading.getTemperature(), reading.getHumidity());
            return;
        }

        if (isBlank(reading.getDeviceName())) {
            reading.setDeviceName("unknown");
        }

        try {
            repository.save(reading);
            log.debug("Saved SensorReading tenant={} devEUI={} fcnt={} temp={} hum={} batt={}",
                    reading.getTenantId(), reading.getDevEui(), reading.getFcnt(),
                    reading.getTemperature(), reading.getHumidity(), reading.getBattery());
        } catch (DataIntegrityViolationException dup) {
            log.debug("Duplicate SensorReading ignored tenant={} devEUI={} fcnt={}",
                    reading.getTenantId(), reading.getDevEui(), reading.getFcnt());
        }
    }

    // -------------------------
    // Helpers
    // -------------------------

    private static String normalizeKey(String key) {
        if (key == null) return null;
        return key.trim()
                .replace("：", ":")
                .replace('/', ' ')
                .replace('_', ' ')
                .toLowerCase(Locale.ROOT)
                .replaceAll("\\s+", " ")
                .trim();
    }

    private static boolean startsWithIgnoreCase(String text, String prefix) {
        return text.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    private static String[] getFromMap(Map<String, String> kv, String... aliases) {
        String[] out = new String[aliases.length];
        for (int i = 0; i < aliases.length; i++) {
            out[i] = kv.get(normalizeKey(aliases[i]));
        }
        return out;
    }

    private static String[] getFirstText(JsonNode node, String... aliases) {
        String[] out = new String[aliases.length];
        for (int i = 0; i < aliases.length; i++) {
            out[i] = asText(node, aliases[i]);
        }
        return out;
    }

    private static Long[] getFirstLong(JsonNode node, String... aliases) {
        Long[] out = new Long[aliases.length];
        for (int i = 0; i < aliases.length; i++) {
            out[i] = getLong(node, aliases[i]);
        }
        return out;
    }

    private static String firstNonBlank(String... values) {
        if (values == null) return null;
        for (String v : values) {
            if (v != null && !v.isBlank()) return v.trim();
        }
        return null;
    }

    private static Long firstNonNull(Long... values) {
        if (values == null) return null;
        for (Long v : values) {
            if (v != null) return v;
        }
        return null;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    private static Long parseLongSafe(String s) {
        if (isBlank(s)) return null;
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException ignore) {
            return null;
        }
    }

    private static Double getDouble(JsonNode node, String field) {
        if (node == null || !node.has(field)) return null;
        JsonNode v = node.get(field);
        if (v.isNumber()) return v.asDouble();
        if (v.isTextual()) {
            try { return Double.parseDouble(v.asText()); } catch (Exception ignore) {}
        }
        return null;
    }

    private static Long getLong(JsonNode node, String field) {
        if (node == null || !node.has(field)) return null;
        JsonNode v = node.get(field);
        if (v.isIntegralNumber()) return v.asLong();
        if (v.isTextual()) {
            try { return Long.parseLong(v.asText()); } catch (Exception ignore) {}
        }
        return null;
    }

    private static String asText(JsonNode node, String field) {
        if (node == null || !node.has(field)) return null;
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) return null;
        return v.asText(null);
    }

    private static String normalizeEui(String eui) {
        if (isBlank(eui)) return null;
        String hex = eui.replaceAll("[^0-9a-fA-F]", "");
        return hex.toUpperCase(Locale.ROOT);
    }


    private static String abbreviate(String s, int max) {
        if (s == null) return null;
        if (s.length() <= max) return s;
        return s.substring(0, Math.max(0, max)) + "...(" + s.length() + " chars)";
    }

    private static String extractGatewayEuiFromRxInfo(JsonNode root) {
        if (root == null || !root.has("rxInfo")) return null;
        JsonNode arr = root.get("rxInfo");
        if (!arr.isArray() || arr.size() == 0) return null;
        JsonNode first = arr.get(0);
        return asText(first, "mac");
    }

    private static String extractGatewayEuiFromTopic(String topic) {
        if (topic == null) return null;
        Matcher m = TOPIC_GWEUI.matcher(topic);
        if (m.matches()) {
            return m.group(1);
        }
        return null;
    }

    // Logging
    private static String formatReading(SensorReading r) {
        if (r == null) return "null";
        return "SensorReading{tenantId=" + r.getTenantId()
                + ", devEui=" + r.getDevEui()
                + ", gwEui=" + r.getGwEui()
                + ", deviceName=" + r.getDeviceName()
                + ", appName=" + r.getAppName()
                + ", fcnt=" + r.getFcnt()
                + ", temperature=" + r.getTemperature()
                + ", humidity=" + r.getHumidity()
                + ", battery=" + r.getBattery()
                + "}";
    }
}
