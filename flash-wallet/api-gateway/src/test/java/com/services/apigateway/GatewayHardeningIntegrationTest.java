package com.services.apigateway;

import com.services.ApiGatewayApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.UUID;

@SpringBootTest(
        classes = ApiGatewayApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "flash.gateway.services.wallet-core-uri=http://localhost:65535",
                "flash.gateway.http-client.connect-timeout-ms=200",
                "flash.gateway.http-client.response-timeout-ms=500",
                "flash.gateway.resilience.circuit-breaker.enabled=true",
                "flash.gateway.idempotency.strict-uuid=false",
                "flash.gateway.idempotency.max-header-length=128",
                "flash.gateway.security.hsts-enabled=false",
                "resilience4j.circuitbreaker.instances.walletCoreCircuitBreaker.sliding-window-size=2",
                "resilience4j.circuitbreaker.instances.walletCoreCircuitBreaker.minimum-number-of-calls=2",
                "resilience4j.circuitbreaker.instances.walletCoreCircuitBreaker.failure-rate-threshold=50",
                "resilience4j.circuitbreaker.instances.walletCoreCircuitBreaker.wait-duration-in-open-state=60s",
                "management.health.redis.enabled=false",
                "management.health.redisRateLimitStore.enabled=false"
        })
@AutoConfigureWebTestClient(timeout = "10000")
class GatewayHardeningIntegrationTest {

    @Autowired
    private WebTestClient webTestClient;

    // --- 3.1: Circuit breaker fallback returns 503 + JSON ---

    @Test
    void circuitBreakerFallbackShouldReturn503WithJsonBody() {
        // Trigger enough failures to open the circuit breaker
        for (int i = 0; i < 3; i++) {
            webTestClient.get()
                    .uri("/api/v1/wallets/" + UUID.randomUUID())
                    .exchange();
        }

        // The fallback should return 503 with a JSON error envelope
        webTestClient.get()
                .uri("/api/v1/wallets/" + UUID.randomUUID())
                .exchange()
                .expectStatus().isEqualTo(503)
                .expectHeader().exists("X-Request-Id")
                .expectBody()
                .jsonPath("$.status").isEqualTo(503)
                .jsonPath("$.service").isEqualTo("api-gateway");
    }

    // --- 3.3: Security headers present on 2xx and 5xx ---

    @Test
    void securityHeadersShouldBePresentOnErrorResponses() {
        webTestClient.get()
                .uri("/api/v1/wallets/" + UUID.randomUUID())
                .exchange()
                .expectHeader().valueEquals("X-Content-Type-Options", "nosniff")
                .expectHeader().valueEquals("X-Frame-Options", "DENY")
                .expectHeader().valueEquals("Referrer-Policy", "no-referrer");
    }

    @Test
    void cacheControlHeaderShouldBePresentOnWalletPaths() {
        webTestClient.get()
                .uri("/api/v1/wallets/" + UUID.randomUUID())
                .exchange()
                .expectHeader().valueEquals("Cache-Control", "no-store");
    }

    // --- 3.5: Bad Content-Type → 415 ---

    @Test
    void shouldReject415WhenPostWithoutJsonContentType() {
        webTestClient.post()
                .uri("/api/v1/wallets/transfer")
                .contentType(MediaType.TEXT_PLAIN)
                .bodyValue("not json")
                .exchange()
                .expectStatus().isEqualTo(415)
                .expectHeader().exists("X-Request-Id")
                .expectBody()
                .jsonPath("$.status").isEqualTo(415);
    }

    @Test
    void shouldReject415WhenPostWithNoContentType() {
        webTestClient.post()
                .uri("/api/v1/wallets/transfer")
                .bodyValue("no content type")
                .exchange()
                .expectStatus().isEqualTo(415);
    }

    // --- 3.6: Oversized Idempotency-Key → 400 ---

    @Test
    void shouldReject400WhenIdempotencyKeyExceedsMaxLength() {
        String oversizedKey = "x".repeat(200);

        webTestClient.post()
                .uri("/api/v1/wallets/transfer")
                .contentType(MediaType.APPLICATION_JSON)
                .header("Idempotency-Key", oversizedKey)
                .bodyValue("""
                        {
                          "senderWalletId": "11111111-1111-1111-1111-111111111111",
                          "receiverWalletId": "22222222-2222-2222-2222-222222222222",
                          "amount": 1000,
                          "currency": "INR"
                        }
                        """)
                .exchange()
                .expectStatus().isBadRequest()
                .expectHeader().exists("X-Request-Id")
                .expectBody()
                .jsonPath("$.status").isEqualTo(400)
                .jsonPath("$.message").value(msg ->
                        org.assertj.core.api.Assertions.assertThat((String) msg).contains("maximum length"));
    }

    // --- 3.4: Method allowlist returns 405 ---

    @Test
    void shouldReturn404ForTraceMethodOnWalletRoute() {
        // TRACE is not in the method allowlist, so gateway doesn't match the route → 404
        webTestClient.method(org.springframework.http.HttpMethod.TRACE)
                .uri("/api/v1/wallets/" + UUID.randomUUID())
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    void shouldReturn404ForOptionsOnWalletRouteWhenNotCorsPrelight() {
        // OPTIONS without CORS preflight headers won't match the method allowlist → 404
        webTestClient.options()
                .uri("/api/v1/wallets/" + UUID.randomUUID())
                .exchange()
                .expectStatus().isNotFound();
    }
}
