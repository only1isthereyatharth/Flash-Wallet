package com.services.apigateway;

import com.services.ApiGatewayApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(
        classes = ApiGatewayApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "flash.gateway.services.wallet-core-uri=http://localhost:65535",
                "flash.gateway.http-client.connect-timeout-ms=200",
                "flash.gateway.http-client.response-timeout-ms=500"
        })
@AutoConfigureWebTestClient
class ApiGatewayIntegrationTest {

    @Autowired
    private WebTestClient webTestClient;

    @Test
    void shouldRejectMissingIdempotencyHeaderBeforeCallingDownstream() {
        webTestClient.post()
                .uri("/api/v1/wallets/transfer")
                .contentType(MediaType.APPLICATION_JSON)
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
                .jsonPath("$.error").isEqualTo("Bad Request")
                .jsonPath("$.path").isEqualTo("/api/v1/wallets/transfer")
                .jsonPath("$.service").isEqualTo("api-gateway");
    }

    @Test
    void shouldReturnGatewayNotFoundEnvelopeForUnknownPath() {
        webTestClient.get()
                .uri("/api/v1/unknown")
                .exchange()
                .expectStatus().isNotFound()
                .expectHeader().exists("X-Request-Id")
                .expectBody()
                .jsonPath("$.status").isEqualTo(404)
                .jsonPath("$.error").isEqualTo("Route Not Found")
                .jsonPath("$.path").isEqualTo("/api/v1/unknown")
                .jsonPath("$.service").isEqualTo("api-gateway");
    }

    @Test
    void shouldReturnServiceUnavailableWhenDownstreamCannotBeReached() {
        webTestClient.get()
                .uri("/api/v1/wallets/11111111-1111-1111-1111-111111111111")
                .exchange()
                .expectStatus().isEqualTo(503)
                .expectHeader().exists("X-Request-Id")
                .expectBody()
                .jsonPath("$.status").isEqualTo(503)
                .jsonPath("$.error").isEqualTo("Downstream Service Unavailable")
                .jsonPath("$.path").isEqualTo("/api/v1/wallets/11111111-1111-1111-1111-111111111111")
                .jsonPath("$.service").isEqualTo("api-gateway");
    }
}
