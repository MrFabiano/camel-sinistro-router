package com.camel.seguros.domain.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.quarkus.runtime.annotations.RegisterForReflection;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;
@RegisterForReflection
@JsonIgnoreProperties(ignoreUnknown = true)
public record SinistroPayload(
        String uuid,
        String apoliceId,
        BigDecimal valorEstimado
) {}