package com.oracle.util;

import java.time.Instant;
import java.util.UUID;

import org.apache.camel.Exchange;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageProcessor {

    private final ObjectMapper mapper = new ObjectMapper();

    private static final String[] required_fields = {
            "StudentID", "StudentName", "age", "AddressLine1", "AddressLine2", "Country"
    };

    private static final String[] address_fields = {
            "AddressLine1", "AddressLine2", "Town", "Country", "Pincode"
    };

    public void receiveValidateTransform(String message, Exchange exchange) throws Exception {
        if (message == null) return;

        JsonNode root;
        if (message.trim().startsWith("<")) {
            root = new XmlMapper().readTree(message.getBytes());
        } else {
            root = mapper.readTree(message);
        }

        // Validation
        validate(root);

        // Transform message
        String payload = transform(root);

        // Use tmisId from JSON if present, otherwise generate
        String tmisId = root.has("tmisId") ? root.get("tmisId").asText() : generateTmisId();

        // JMS correlationId
        String correlationId = exchange.getIn().getHeader("JMSMessageID") != null ?
                exchange.getIn().getHeader("JMSMessageID").toString() :
                generateCorrelationId();

        exchange.getIn().setHeader("tmisId", tmisId);
        exchange.getIn().setHeader("correlationId", tmisId);
        exchange.getIn().setHeader("payload", payload);

        log.info("[MessageProcessor] tmisId={}, correlationId={}", tmisId, correlationId);
    }

    private void validate(JsonNode root) {
        log.info("[Validation] Checking required fields...");
        for (String field : required_fields) require(root, field);
        log.info("[Validation] All required fields present.");
    }

    private void require(JsonNode root, String field) {
        if (!root.hasNonNull(field)) {
            log.error("[Validation] Missing required field: {}", field);
            throw new IllegalArgumentException("Missing required field: " + field);
        }
    }

    private String transform(JsonNode root) throws Exception {
        ObjectNode newJson = mapper.createObjectNode();
        ArrayNode fullAddress = mapper.createArrayNode();
        for (String field : address_fields) {
            JsonNode node = root.get(field);
            if (node != null && !node.isNull()) {
                String val = node.asText();
                if (!val.isEmpty()) fullAddress.add(val);
            }
        }

        newJson.set("StudentID", root.get("StudentID"));
        newJson.set("StudentName", root.get("StudentName"));
        newJson.set("age", root.get("age"));
        newJson.set("AddressLine", fullAddress);

        String newJsonString = mapper.writeValueAsString(newJson);
        log.info("[Transform] Transformed JSON: {}", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(newJson));
        return newJsonString;
    }

    public String generateTmisId() {
        return "TMIS-" + Instant.now().toEpochMilli();
    }

    public static String generateCorrelationId() {
        return UUID.randomUUID().toString();
    }
}
