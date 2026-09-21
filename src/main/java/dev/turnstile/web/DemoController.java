package dev.turnstile.web;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Starts a live rush and reports on it. Staff only when security is on. */
@RestController
@RequestMapping("/api/demo")
@ConditionalOnProperty(name = "turnstile.demo.enabled", havingValue = "true", matchIfMissing = true)
public class DemoController {

  public record RushRequest(Integer seats, Integer buyers) {}

  private final RushService rush;

  public DemoController(RushService rush) {
    this.rush = rush;
  }

  @PostMapping("/rush")
  public ResponseEntity<RushService.Result> start(@RequestBody(required = false) RushRequest request) {
    int seats = request == null || request.seats() == null ? 100 : request.seats();
    int buyers = request == null || request.buyers() == null ? 1_000 : request.buyers();
    try {
      return ResponseEntity.status(HttpStatus.ACCEPTED).body(rush.start(seats, buyers));
    } catch (IllegalStateException busy) {
      return ResponseEntity.status(HttpStatus.CONFLICT).build();
    }
  }

  @GetMapping("/rush/{runId}")
  public ResponseEntity<RushService.Result> get(@PathVariable String runId) {
    RushService.Result result = rush.result(Ids.require("runId", runId));
    return result == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(result);
  }
}
