package dev.turnstile.web;

import dev.turnstile.PricingProperties;
import dev.turnstile.saga.PurchaseSaga;
import dev.turnstile.saga.SagaRecord;
import java.security.Principal;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Buying a seat over HTTP.
 *
 * <p>The {@code Idempotency-Key} header is the README's "clients retry" promise
 * made concrete: a buyer whose connection drops resends the same request with the
 * same key and gets the same purchase back, never a second seat and never a
 * spurious failure. Keys are scoped to the buyer, so one buyer cannot read or
 * replay another's purchase by guessing a key.
 */
@RestController
@RequestMapping("/api/purchases")
public class PurchaseController {

  public record PurchaseRequest(String seatId, String buyerId, Long amountCents) {}

  public record PurchaseResponse(
      String purchaseId, String seatId, String state, String detail, String orderId) {

    static PurchaseResponse of(String purchaseId, SagaRecord r) {
      return new PurchaseResponse(purchaseId, r.seatId(), r.state().name(), r.detail(), r.orderId());
    }
  }

  private final PurchaseSaga saga;
  private final PricingProperties pricing;

  private final io.micrometer.core.instrument.MeterRegistry metrics;

  public PurchaseController(PurchaseSaga saga, PricingProperties pricing, io.micrometer.core.instrument.MeterRegistry metrics) {
    this.metrics = metrics;
    this.saga = saga;
    this.pricing = pricing;
  }

  @PostMapping
  public ResponseEntity<PurchaseResponse> buy(
      @RequestBody PurchaseRequest request,
      @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
      Principal principal) {

    String seatId = Ids.require("seatId", request.seatId());
    // An authenticated caller is who the token says, whatever the body claims.
    String buyerId =
        principal != null && !"anonymousUser".equals(principal.getName())
            ? principal.getName()
            : Ids.require("buyerId", request.buyerId());
    Ids.require("buyerId", buyerId);

    long amount = request.amountCents() == null ? pricing.seatPriceCents() : request.amountCents();
    if (amount < 0 || amount > 10_000_000) {
      throw new IllegalArgumentException("amountCents is out of range");
    }

    String key = idempotencyKey == null ? UUID.randomUUID().toString() : Ids.require("Idempotency-Key", idempotencyKey);
    String sagaId = buyerId + ":" + key;

    long began = System.nanoTime();
    SagaRecord result = saga.start(sagaId, seatId, buyerId, amount);
    io.micrometer.core.instrument.Timer.builder("turnstile.purchase")
        .description("End-to-end purchase time, by outcome")
        .tag("state", result.state().name())
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(metrics)
        .record(System.nanoTime() - began, java.util.concurrent.TimeUnit.NANOSECONDS);
    return ResponseEntity.status(statusFor(result)).body(PurchaseResponse.of(key, result));
  }

  /** Look a purchase up again, e.g. after losing the response. Only its own buyer can. */
  @GetMapping("/{key}")
  public ResponseEntity<PurchaseResponse> find(
      @PathVariable String key, @RequestParam(required = false) String buyerId, Principal principal) {
    String buyer =
        principal != null && !"anonymousUser".equals(principal.getName())
            ? principal.getName()
            : Ids.require("buyerId", buyerId);
    return saga.find(buyer + ":" + Ids.require("key", key))
        .map(r -> ResponseEntity.status(statusFor(r)).body(PurchaseResponse.of(key, r)))
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  private static HttpStatus statusFor(SagaRecord r) {
    return switch (r.state()) {
      case CONFIRMED -> HttpStatus.CREATED;
      case REFUSED, REFUNDED -> HttpStatus.CONFLICT;
      case DECLINED -> HttpStatus.PAYMENT_REQUIRED;
      default -> HttpStatus.ACCEPTED;
    };
  }
}
