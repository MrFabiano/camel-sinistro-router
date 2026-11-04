package com.camel.seguros.domain.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record ContingenciaResponseDTO(String status, String requestId) {}