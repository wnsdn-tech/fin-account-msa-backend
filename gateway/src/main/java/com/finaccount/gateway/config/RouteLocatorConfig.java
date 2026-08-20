package com.finaccount.gateway.config;

import com.finaccount.gateway.filter.JwtAuthenticationFilter;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RouteLocatorConfig {

    private final JwtAuthenticationFilter jwtAuthenticationFilter;

    public RouteLocatorConfig(JwtAuthenticationFilter jwtAuthenticationFilter) {
        this.jwtAuthenticationFilter = jwtAuthenticationFilter;
    }

    @Bean
    public RouteLocator getRouteLocator(RouteLocatorBuilder builder) {
        return builder.routes()

                .route("account-service-login", route -> route
                        .path("/auth/login/**")
                        .filters(filter -> filter
                                .rewritePath(
                                        "/auth/(?<segment>.*)",
                                        "/${segment}"
                                )
                        )
                        .uri("lb://ACCOUNT-SERVICE")
                )

                .route("account-service", route -> route
                        .path("/accounts/**")
                        .filters(filter -> filter
                                .filter(jwtAuthenticationFilter)
                        )
                        .uri("lb://ACCOUNT-SERVICE")
                )

                .route("transaction-service", route -> route
                        .path("/transactions/**")
                        .filters(filter -> filter
                                .filter(jwtAuthenticationFilter)
                        )
                        .uri("lb://TRANSACTION-SERVICE")
                )

                .route("notification-service", route -> route
                        .path("/notifications/**")
                        .filters(filter -> filter
                                .filter(jwtAuthenticationFilter)
                        )
                        .uri("lb://NOTIFICATION-SERVICE")
                )

                .route("account-service-h2console", route -> route
                        .path("/account-service/h2-console/**")
                        .filters(filter -> filter
                                .rewritePath(
                                        "/account-service/(?<segment>.*)",
                                        "/${segment}"
                                )
                        )
                        .uri("lb://ACCOUNT-SERVICE")
                )

                .route("transaction-service-h2console", route -> route
                        .path("/transaction-service/h2-console/**")
                        .filters(filter -> filter
                                .rewritePath(
                                        "/transaction-service/(?<segment>.*)",
                                        "/${segment}"
                                )
                        )
                        .uri("lb://TRANSACTION-SERVICE")
                )

                .route("notification-service-h2console", route -> route
                        .path("/notification-service/h2-console/**")
                        .filters(filter -> filter
                                .rewritePath(
                                        "/notification-service/(?<segment>.*)",
                                        "/${segment}"
                                )
                        )
                        .uri("lb://NOTIFICATION-SERVICE")
                )

                .build();
    }
}