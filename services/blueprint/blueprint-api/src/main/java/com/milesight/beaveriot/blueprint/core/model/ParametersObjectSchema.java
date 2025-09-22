package com.milesight.beaveriot.blueprint.core.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.milesight.beaveriot.base.utils.JsonUtils;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;


@Data
@NoArgsConstructor
public class ParametersObjectSchema {

    private String type = "object";

    private JsonNode properties;

    private List<String> required;

    public ParametersObjectSchema(JsonNode properties) {
        this.properties = JsonUtils.copy(properties);
        this.required = new ArrayList<>();
        this.properties.properties().forEach(prop -> {
            if (prop.getValue() instanceof ObjectNode obj
                    && (!(obj.get("required") instanceof BooleanNode isRequired)
                    || !isRequired.booleanValue())) {
                this.required.add(prop.getKey());
                obj.remove("required");
            }
        });
        if (this.required.isEmpty()) {
            this.required = null;
        }
    }

}
