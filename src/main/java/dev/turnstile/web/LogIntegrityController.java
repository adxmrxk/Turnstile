package dev.turnstile.web;

import dev.turnstile.eventstore.ChainVerifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Tamper evidence for the durable log. Only present with the PostgreSQL store,
 * since the in-memory store keeps no chain and dies with the process anyway.
 *
 * <p>Fetch a checkpoint on a schedule and keep it somewhere the database's owner
 * cannot write. Later, post it back to verify: the chain proves internal
 * consistency, and the checkpoint proves nothing was rewritten or truncated since.
 */
@RestController
@RequestMapping("/api/log")
@ConditionalOnProperty(name = "turnstile.store", havingValue = "postgres")
public class LogIntegrityController {

  private final ChainVerifier verifier;

  public LogIntegrityController(ChainVerifier verifier) {
    this.verifier = verifier;
  }

  @GetMapping("/checkpoint")
  public ChainVerifier.Checkpoint checkpoint() {
    return verifier.checkpoint();
  }

  /** With a checkpoint in the body, also checks the log still contains everything it recorded. */
  @PostMapping("/verify")
  public ChainVerifier.Report verify(@RequestBody(required = false) ChainVerifier.Checkpoint checkpoint) {
    return checkpoint == null ? verifier.verify() : verifier.verifyAgainst(checkpoint);
  }
}
