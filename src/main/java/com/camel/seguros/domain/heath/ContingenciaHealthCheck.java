package com.camel.seguros.domain.heath;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

@ApplicationScoped
@Readiness
public class ContingenciaHealthCheck implements HealthCheck {

    private static final int THRESHOLD_PENDENCIAS = 100;

    @Inject
    DataSource dataSource;

    @Override
    public HealthCheckResponse call() {
        try (Connection connection = dataSource.getConnection()) {
            String sql = "SELECT COUNT(*) FROM eventopendente WHERE processado = false";

            try (PreparedStatement stmt = connection.prepareStatement(sql);
                 ResultSet rs = stmt.executeQuery()) {

                if (rs.next()) {
                    long pendentes = rs.getLong(1);

                    if (pendentes > THRESHOLD_PENDENCIAS) {
                        return HealthCheckResponse
                                .named("Eventos Pendentes (Camel Contingência)")
                                .down()
                                .withData("count", String.valueOf(pendentes))
                                .withData("threshold", String.valueOf(THRESHOLD_PENDENCIAS))
                                .withData("status", "HIGH")
                                .build();
                    }

                    return HealthCheckResponse
                            .named("Eventos Pendentes (Camel Contingência)")
                            .up()
                            .withData("count", String.valueOf(pendentes))
                            .withData("threshold", String.valueOf(THRESHOLD_PENDENCIAS))
                            .withData("status", "NORMAL")
                            .build();
                }
            }
        } catch (Exception e) {
            return HealthCheckResponse
                    .named("Eventos Pendentes (Camel Contingência)")
                    .down()
                    .withData("error", e.getMessage())
                    .build();
        }

        return HealthCheckResponse
                .named("Eventos Pendentes (Camel Contingência)")
                .down()
                .withData("message", "Não foi possível consultar o banco")
                .build();
    }
}

