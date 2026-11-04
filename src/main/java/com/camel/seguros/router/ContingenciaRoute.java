package com.camel.seguros.router;

import com.camel.seguros.domain.model.ContingenciaResponseDTO;
import com.camel.seguros.domain.model.SinistroPayload;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.LoggingLevel;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.camel.model.rest.RestParamType;

import java.util.UUID;

@ApplicationScoped
public class ContingenciaRoute extends RouteBuilder {

    @Override
    public void configure() throws Exception {

        restConfiguration()
                .apiProperty("api.title", "Camel Router - Sinistro")
                .apiProperty("api.version", "1.0.0");

        rest("/api/v1")
                .description("Caminho alternativo de publicação de eventos de sinistro")
                .post("/contingencia")
                .description("Recebe o evento de contingência e tenta publicá-lo no Kafka")
                .consumes("application/json")
                .produces("application/json")
                .type(SinistroPayload.class)
                .outType(ContingenciaResponseDTO.class)
                .param().name("body").type(RestParamType.body)
                .description("Evento do sinistro (uuid, apoliceId e valorEstimado)").required(true).endParam()
                .responseMessage().code(200)
                .message("Processado: status 'processed' (publicado) ou 'failed' (Kafka indisponível)")
                .responseModel(ContingenciaResponseDTO.class).endResponseMessage()
                .responseMessage().code(400).message("apoliceId é obrigatório").endResponseMessage()
                .to("direct:processar-contingencia");

        from("direct:processar-contingencia")
                .routeId("contingencia-receive-route")
                .log(LoggingLevel.INFO, "Recebendo contingência do Sinistro API: ${body}")
                .unmarshal().json(JsonLibrary.Jackson, SinistroPayload.class)

                .process(exchange -> {
                    SinistroPayload payload = exchange.getIn().getBody(SinistroPayload.class);
                    String requestId = (payload != null && payload.uuid() != null)
                            ? payload.uuid()
                            : UUID.randomUUID().toString();
                    exchange.getMessage().setHeader("requestId", requestId);
                })

                .choice()
                .when(simple("${body.apoliceId} == null"))
                .setHeader("CamelHttpResponseCode", constant(400))
                .setBody(constant("{\"error\":\"apoliceId é obrigatório\"}"))
                .otherwise()
                .doTry()
                .marshal().json(JsonLibrary.Jackson)
                //.to("kafka:seguros-sinistros-topic?maxBlockMs=5000")
                .to("kafka:{{app.kafka.topic}}?maxBlockMs=2000")
                .setHeader("status", constant("processed"))
                .doCatch(Exception.class)
                .log(LoggingLevel.WARN, "Falha no Kafka. Devolvendo failed ao chamador.")
                .setHeader("status", constant("failed"))
                .end()
                .setHeader("CamelHttpResponseCode", constant(200))
                .setBody(simple("{\"status\":\"${header.status}\",\"requestId\":\"${header.requestId}\"}"))
                .end()
                .setHeader("Content-Type", constant("application/json"));
    }
}