package com.camel.seguros.router;

import com.camel.seguros.domain.dto.ContingenciaResponseDTO;
import com.camel.seguros.domain.model.SinistroPayload;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

@RegisterRestClient(configKey = "camel-router")
public interface CamelRouterClient {

    @POST
    @Path("/api/v1/contingencia")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    ContingenciaResponseDTO enviarContingencia(SinistroPayload payload);
}