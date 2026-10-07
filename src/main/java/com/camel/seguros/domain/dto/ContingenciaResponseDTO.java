package com.camel.seguros.domain.dto;

public record ContingenciaResponseDTO(
        String status,
        String mensagem,
        String requestId,
        boolean aceito
) {}