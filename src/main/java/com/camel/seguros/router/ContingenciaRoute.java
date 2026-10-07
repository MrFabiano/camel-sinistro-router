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
                .log(LoggingLevel.INFO, "Recebendo contingência do Sinistro API: ${body}")
                .unmarshal().json(JsonLibrary.Jackson, SinistroPayload.class)
                .process(exchange -> {
                    SinistroPayload payload = exchange.getIn().getBody(SinistroPayload.class);
                    String requestId = (payload != null && payload.uuid() != null)
                            ? payload.uuid().toString()
                            : UUID.randomUUID().toString();

                    exchange.getMessage().setHeader("requestId", requestId);
                })

                .choice()
                .when(simple("${body.apoliceId} == null"))
                .setHeader("error", constant("apoliceId é obrigatório"))
                .setHeader("CamelHttpResponseCode", constant(400))
                .setBody(simple("{\"error\":\"${header.error}\"}"))
                .otherwise()
                .doTry()
                .marshal().json(JsonLibrary.Jackson)
                //.to("kafka:seguros-sinistros-topic?brokers={{kafka.bootstrap.servers}}")
                .to("kafka:seguros-sinistros-topic")
                .log(LoggingLevel.INFO, "Contingência publicada no Kafka com sucesso")
                .setHeader("status", constant("processed"))
                .doCatch(Exception.class)
                .log(LoggingLevel.WARN, "Falha no Kafka. Gravando em contingência no banco...")
                // Uso do #quarkus para referência de Bean
                //.to("sql:INSERT INTO evento_pendente (uuid, payload, processado) VALUES (:#${header.requestId}, :#${body}, false)?dataSource=#quarkus")
                .to("sql:INSERT INTO evento_pendente (uuid, payload, processado) VALUES (:#${header.requestId}, :#${body}, false)")
                .setHeader("status", constant("pending"))
                .end()

                .setHeader("CamelHttpResponseCode", constant(200))
                .setBody(simple("{\"status\":\"${header.status}\",\"requestId\":\"${header.requestId}\"}"))
                .end()
                .setHeader("Content-Type", constant("application/json"));
    }
}