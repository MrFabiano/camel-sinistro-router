package com.camel.seguros.domain.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

public record SinistroPayload(
        Long id,
        UUID uuid,
        String apoliceId,
        String descricao,
        BigDecimal valorEstimado,
        String status,
        LocalDateTime dataCriacao
) {}