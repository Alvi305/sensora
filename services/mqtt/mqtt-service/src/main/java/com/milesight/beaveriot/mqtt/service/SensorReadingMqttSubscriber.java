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

import static java.lang.Long.getLong;

@Slf4j
@Component
@RequiredArgsConstructor
public class SensorReadingMqttSubscriber {

    private final MqttPubSubService mqtt; // pub/sub bridge
    private final SensorReadingsRepository repository;
    private final ObjectMapper mapper = new ObjectMapper();

    private static final Pattern KV_COLON = Pattern.compile("^\\s*([^:]+?)\\s*[:：]\\s*(.+)\\s*$");
    private static final Pattern JSON_BLOCK_PATTERN = Pattern.compile("\\{[\\s\\S]*}$");

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
        String text = new String(msg.getPayload(), StandardCharsets.UTF_8).trim();

        Map<String, String> kv = parseKeyValues(text);

        JsonNode node = extractJson(text);

        // Build SensorReading
        SensorReading reading = new SensorReading();
        reading.setTenantId(msg.getTenantId());

// 1) Extract identifiers from KV (existing behavior)
        reading.setDevEui(firstNonNull(
                kv.get("device eui"),
                kv.get("device eui group name"),
                kv.get("device eui/group name"),
                kv.get("deveui"),
                kv.get("dev eui"),
                kv.get("dev_eui")
        ));

        reading.setGwEui(firstNonNull(
                kv.get("gweui"),
                kv.get("gw eui"),
                kv.get("gateway eui"),
                kv.get("gw_eui")
        ));

        reading.setAppEui(firstNonNull(
                kv.get("appeui"),
                kv.get("app eui"),
                kv.get("app_eui")
        ));

        reading.setDevAddr(firstNonNull(
                kv.get("dev addr multicast addr"),
                kv.get("dev addr"),
                kv.get("multicast addr"),
                kv.get("dev_addr")
        ));

        reading.setFcnt(parseLong(firstNonNull(
                kv.get("fcnt"),
                kv.get("f cnt")
        )));

        // 2) Telemetry from top-level JSON (existing)
        reading.setTemperature(getDouble(node, "temperature"));
        reading.setHumidity(getDouble(node, "humidity"));
        reading.setBattery(getDouble(node, "battery"));


        // 3) Extract identifiers and fields from JSON aliases (snake_case + camelCase)
        if (node != null) {
            if (isBlank(reading.getDevEui())) {
                reading.setDevEui(firstNonNull(
                        asText(node, "dev_eui"),
                        asText(node, "deveui"),
                        asText(node, "devEui"),
                        asText(node, "deviceEui")
                ));
            }
            if (isBlank(reading.getGwEui())) {
                reading.setGwEui(firstNonNull(
                        asText(node, "gw_eui"),
                        asText(node, "gweui"),
                        asText(node, "gwEui"),
                        asText(node, "gatewayEui")
                ));
            }
            if (isBlank(reading.getAppEui())) {
                reading.setAppEui(firstNonNull(
                        asText(node, "app_eui"),
                        asText(node, "appeui"),
                        asText(node, "appEui")
                ));
            }
            if (isBlank(reading.getDevAddr())) {
                reading.setDevAddr(firstNonNull(
                        asText(node, "dev_addr"),
                        asText(node, "devAddr")
                ));
            }

//            if (reading.getFcnt() == null) {
//                Long fcntFromJson = firstNonNull(
//                        getLong(node, "fcnt"),
//                        getLong(node, "FCnt")
//                );
//                reading.setFcnt(fcntFromJson);
//            }

            // If telemetry may be nested under "payload", merge it in
            if (node.has("payload")) {
                JsonNode p = node.get("payload");
                if (reading.getTemperature() == null) reading.setTemperature(getDouble(p, "temperature"));
                if (reading.getHumidity() == null) reading.setHumidity(getDouble(p, "humidity"));
                if (reading.getBattery() == null) reading.setBattery(getDouble(p, "battery"));
            }
        }

        // 4) Normalize EUIs (uppercase, strip separators) but null-safe
        reading.setDevEui(stdEui(reading.getDevEui()));
        reading.setGwEui(stdEui(reading.getGwEui()));
        reading.setAppEui(stdEui(reading.getAppEui()));

        // 5) Validate required fields
        if (isBlank(reading.getTenantId()) || isBlank(reading.getDevEui())) {
            log.warn("Missing tenantId/devEui; skipping persist. tenantId={} devEui={} topic={}",
                    reading.getTenantId(), reading.getDevEui(), msg.getFullTopicName());
            return;
        }

        // 6) Persist
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

    // Helper Methods
    private Map<String, String> parseKeyValues(String text)
    {
        Map<String, String> map = new LinkedHashMap<>();
        for (String rawLine : text.split("\\r?\\n")) {
            String line = rawLine == null ? "" : rawLine.trim();
            if (line.isEmpty()) continue;

            // JSON block starts; stop parsing KV
            if (line.startsWith("{")) break;

            // Try "Key: Value"
            Matcher m = KV_COLON.matcher(line);
            if (m.matches()) {
                String key = normalizeKey(m.group(1));
                String val = m.group(2).trim();
                map.put(key, val);
                continue;
            }

            // Concatenated pattern like "Dev Addr/Multicast AddrF2071142"
            String[] knownKeys = new String[] {
                    "Dev Addr/Multicast Addr",
                    "Dev Addr",
                    "Multicast Addr",
                    "GwEUI",
                    "AppEUI",
                    "Device EUI/Group Name",
                    "Device EUI",
                    "Fcnt",
                    "FCnt",
                    "Port",
                    "MIC",
                    "Class Type",
                    "Modulation",
                    "Bandwidth",
                    "SpreadFactor",
                    "Bitrate",
                    "CodeRate",
                    "SNR",
                    "RSSI"
            };

            boolean captured = false;
            for (String k : knownKeys) {
                if (line.toLowerCase(Locale.ROOT).startsWith(k.toLowerCase(Locale.ROOT))) {
                    String key = normalizeKey(k);
                    String val = line.substring(k.length()).trim();
                    map.put(key, val);
                    captured = true;
                    break;
                }
            }
            if (!captured) {
                // Not a KV line, ignore (could be headers like "Packet Details", "JSON", etc.)
            }
        }
        return map;
    }

    private String normalizeKey(String k)
    {
        if (k == null) return null;
        return k.trim()
                .replace("：", ":")
                .replace('/', ' ')
                .replace('_', ' ')
                .toLowerCase(Locale.ROOT)
                .replaceAll("\\s+", " ")
                .trim();
    }

    private JsonNode extractJson(String text)
    {
        try {
            if (text.startsWith("{") && text.endsWith("}")) {
                return mapper.readTree(text);
            }
        } catch (JsonProcessingException e) {
            log.warn("Failed to parse full string as JSON: {}", text, e); // Log the exception with the input for debugging
        }

        Matcher m = JSON_BLOCK_PATTERN.matcher(text);
        if (m.find()) {
            String matchedJson = m.group(0);
            try {
                return mapper.readTree(matchedJson);
            } catch (JsonProcessingException e) {
                log.warn("Failed to parse matched JSON block: {}", matchedJson, e); // Log the exception with the matched input
            }
        }

        log.debug("No valid JSON found in input: {}", text); // Optional debug log for failure cases
        return null;
    }

    private static String firstNonNull(String... values)
    {
        if (values == null) return null;
        for (String v : values) if (v != null && !v.isBlank()) return v.trim();
        return null;
    }

    private static boolean isBlank(String s)
    {
        return s == null || s.isBlank();
    }

    private static Long parseLong(String s)
    {
        if (s == null || s.isBlank()) return null;
        try { return Long.parseLong(s.trim()); } catch (Exception e) { return null; }
    }

    private static Double getDouble(JsonNode n, String field) {
        if (n == null || !n.has(field)) return null;
        JsonNode v = n.get(field);
        if (v.isNumber()) return v.asDouble();
        if (v.isTextual()) {
            try { return Double.parseDouble(v.asText()); } catch (Exception ignore) {}
        }
        return null;
    }

    private static Long getLong(JsonNode n, String field)
    {
        if (n == null || !n.has(field)) return null;
        JsonNode v = n.get(field);
        if (v.isIntegralNumber()) return v.asLong();
        if (v.isTextual()) {
            try { return Long.parseLong(v.asText()); } catch (Exception ignore) {}
        }
        return null;
    }

    private static String asText(JsonNode n, String field)
    {
        if (n == null || !n.has(field)) return null;
        JsonNode v = n.get(field);
        if (v == null || v.isNull()) return null;
        return v.asText(null);
    }

    private static String stdEui(String eui) {
        if (eui == null || eui.isBlank()) return null;
        String cleaned = eui.replaceAll("[^0-9a-fA-F]", "");
        // Accept 12–16 hex chars; if not, just uppercase cleaned for visibility
        if (cleaned.matches("^[0-9a-fA-F]{12,16}$")) {
            return cleaned.toUpperCase(Locale.ROOT);
        }
        return cleaned.toUpperCase(Locale.ROOT);
    }
}
