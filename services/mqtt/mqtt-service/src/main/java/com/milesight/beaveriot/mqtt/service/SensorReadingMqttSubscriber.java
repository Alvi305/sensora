package com.milesight.beaveriot.mqtt.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milesight.beaveriot.mqtt.repository.SensorReadingsRepository;
import com.milesight.beaveriot.mqtt.service.MqttPubSubService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

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

}
