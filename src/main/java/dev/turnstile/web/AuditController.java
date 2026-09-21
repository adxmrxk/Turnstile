package dev.turnstile.web;

import dev.turnstile.audit.LogAuditor;
import dev.turnstile.audit.NdjsonExport;
import dev.turnstile.eventstore.EventStore;
import dev.turnstile.eventstore.NotifyingEventStore;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The live log, exposed for audit.
 *
 * <p>{@code /api/export} is the missing half of the systemd audit timer: it
 * serves the real log in the exact NDJSON the independent awk verifier reads, so
 * {@code turnstilectl export | verify-invariants.sh} checks the running system's
 * own log with no shared code. {@code /api/audit} runs the Java twin of that
 * verifier in-process for a quick answer.
 */
@RestController
@RequestMapping("/api")
public class AuditController {

  private final EventStore store;

  public AuditController(NotifyingEventStore store) {
    this.store = store;
  }

  @GetMapping(value = "/export", produces = "application/x-ndjson")
  public ResponseEntity<byte[]> export() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    NdjsonExport.write(store.readAll(), out);
    return ResponseEntity.ok().contentType(MediaType.parseMediaType("application/x-ndjson")).body(out.toByteArray());
  }

  @GetMapping("/audit")
  public LogAuditor.Report audit() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    NdjsonExport.write(store.readAll(), out);
    List<String> lines = Arrays.asList(out.toString(StandardCharsets.UTF_8).split("\n"));
    return new LogAuditor().audit(lines);
  }
}
