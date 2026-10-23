package com.camel.seguros.router;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.LoggingLevel;

@ApplicationScoped
public class ReprocessamentoRoute extends RouteBuilder {

    @Override
    public void configure() throws Exception {

        // Escopo: vale para todas as rotas deste RouteBuilder
        onException(Exception.class)
                .process(exchange -> {
                    Exception ex = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                    exchange.getMessage().setHeader("erro", ex != null ? ex.getMessage() : "desconhecido");
                })
                .marshal().json()
                .to("kafka:seguros-sinistros-dlq");

        from("timer:reprocessar-pendentes?period=300000")
                .routeId("reprocessamento-route")
                .autoStartup(false)
                .log(LoggingLevel.INFO, "Iniciando busca de pendentes para reprocessamento")

                // SELECT usa id (nao uuid, que nao existe)
                .to("sql:SELECT id, payloadjson FROM eventopendente WHERE processado = false LIMIT 100")

                .split(body())
                .setHeader("pendenteId", simple("${body[id]}"))

                .process(exchange -> {
                    Object row = exchange.getIn().getBody();
                    if (row instanceof java.util.Map<?, ?> map) {
                        Object json = map.get("payloadjson");
                        exchange.getIn().setBody(json != null ? json.toString() : null);
                    }
                })

                .to("kafka:seguros.sinistros.events")
                .log(LoggingLevel.INFO, "Pendente ${header.pendenteId} reprocessado com sucesso")

                // UPDATE usa id (nao uuid)
                .to("sql:UPDATE eventopendente SET processado = true WHERE id = :#pendenteId")
                .end();
    }
}
