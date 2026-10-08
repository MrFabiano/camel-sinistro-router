package com.camel.seguros.router;

import com.camel.seguros.processor.PendenteProcessor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.LoggingLevel;

@ApplicationScoped
public class ReprocessamentoRoute extends RouteBuilder {

    @Override
    public void configure() throws Exception {

        // Tratamento global de exceções para DLQ
        onException(Exception.class)
                .process(exchange -> {
                    Exception ex = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);

                    assert ex != null;
                    exchange.getMessage().setHeader("erro", ex.getMessage());
                })
                .marshal().json()
                .to("kafka:seguros-sinistros-dlq");
//                .maximumRedeliveries(3)
//                .redeliveryDelay(2000)
//                .backOffMultiplier(2)
//                .retryAttemptedLogLevel(LoggingLevel.WARN)
//                .handled(true)
//                .log(LoggingLevel.ERROR, "Falha ao reprocessar pendente ${header.pendenteUuid}. Enviando para DLQ.")
                // Removido ?brokers={{...}} - Camel resolve via camel.component.kafka.brokers


        from("timer:reprocessar-pendentes?period=300000")
                .routeId("reprocessamento-route")
                .autoStartup(false)
                .log(LoggingLevel.INFO, "Iniciando busca de pendentes para reprocessamento")

                .to("sql:SELECT uuid, payload FROM eventopendente WHERE processado = false LIMIT 100")

                .split(body())
                .setHeader("pendenteUuid", simple("${body[uuid]}"))

                // Chama o Processor CDI
                .process("pendenteProcessor")

                // Removido ?brokers={{...}} - Camel resolve via camel.component.kafka.brokers
                .to("kafka:seguros-sinistros-topic")

                // Parâmetro SQL corrigido para o Camel e CAST para UUID no PostgreSQL
                .to("sql:UPDATE eventopendente SET processado = true WHERE uuid = CAST(:#pendenteUuid AS uuid)")

                .log(LoggingLevel.INFO, "Registro ${header.pendenteUuid} reprocessado com sucesso")
                .end();
    }
}