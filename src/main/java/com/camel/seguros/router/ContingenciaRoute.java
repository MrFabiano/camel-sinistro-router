package com.camel.seguros.router;

import com.camel.seguros.domain.model.SinistroPayload;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.LoggingLevel;
import org.apache.camel.model.dataformat.JsonLibrary;

import java.util.UUID;

@ApplicationScoped
public class ContingenciaRoute extends RouteBuilder {

    @Override
    public void configure() throws Exception {

        rest("/api/v1")
                .post("/contingencia")
                .consumes("application/json")
                .produces("application/json")
                .type(SinistroPayload.class)
                .to("direct:processar-contingencia");

        from("direct:processar-contingencia")
                .routeId("contingencia-receive-route")

                .log(LoggingLevel.INFO, "=== RECEBEU CONTINGENCIA ===")
                .log(LoggingLevel.DEBUG, "Raw Body: ${body}")

                .unmarshal().json(JsonLibrary.Jackson, SinistroPayload.class)
                .log(LoggingLevel.INFO, "Payload recebido: apoliceId=${body.apoliceId}, valor=${body.valorEstimado}")

                .process(exchange -> {
                    SinistroPayload payload = exchange.getIn().getBody(SinistroPayload.class);
                    String requestId = (payload != null && payload.uuid() != null)
                            ? String.valueOf(payload.uuid())
                            : UUID.randomUUID().toString();

                    exchange.getMessage().setHeader("requestId", requestId);
                    exchange.getMessage().setHeader("status", "pending");
                })

                .choice()
                .when(simple("${body.apoliceId} == null"))
                .setHeader("CamelHttpResponseCode", constant(400))
                .setBody(constant("{\"error\":\"apoliceId e obrigatorio\"}"))
                .otherwise()
                .doTry()
                .log(LoggingLevel.INFO, "Publicando no Kafka topic: seguros.sinistros.events")
                .marshal().json(JsonLibrary.Jackson)
                .to("kafka:seguros.sinistros.events")
                .log(LoggingLevel.INFO, "Contingencia publicada no Kafka | requestId: ${header.requestId}")
                .setHeader("status", constant("processed"))
                .doCatch(Exception.class)
                .log(LoggingLevel.WARN, "Falha no Kafka. Gravando contingencia localmente...")
                .to("sql:INSERT INTO eventopendente (payloadjson, datacriacao, processado) "
                        + "VALUES (:#${body}, CURRENT_TIMESTAMP, false)")
                .log(LoggingLevel.INFO, "Gravado em eventopendente | requestId: ${header.requestId}")
                .setHeader("status", constant("pending_local"))
                .end()
                .setHeader("CamelHttpResponseCode", constant(200))
                .setBody(simple("{\"status\":\"${header.status}\",\"requestId\":\"${header.requestId}\"}"))
                .end()
                .setHeader("Content-Type", constant("application/json"));
    }
}