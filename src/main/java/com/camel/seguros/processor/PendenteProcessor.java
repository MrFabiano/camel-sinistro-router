package com.camel.seguros.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.inject.Named;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.enterprise.context.ApplicationScoped;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

@Named("pendenteProcessor")
@ApplicationScoped
public class PendenteProcessor implements Processor {

    private static final Logger LOG = LoggerFactory.getLogger(PendenteProcessor.class);

    @Override
    public void process(Exchange exchange) throws Exception {
        Map<String, Object> pendente = exchange.getIn().getBody(Map.class);

        if (pendente != null) {
            LOG.info("Processando pendente UUID: {}", pendente.get("uuid"));

            // Caso o payload esteja como String/JSON no Map do SQL
            Object payload = pendente.get("payload");
            if (payload != null) {
                exchange.getIn().setBody(payload.toString());
            }
        }
    }
}