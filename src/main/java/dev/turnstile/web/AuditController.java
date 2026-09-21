package dev.turnstile.web;

import dev.turnstile.audit.LogAuditor;
import dev.turnstile.audit.NdjsonExport;
import dev.turnstile.eventstore.EventStore;
import dev.turnstile.eventstore.NotifyingEventStore;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

/**
 * The live log, exposed for audit.
 *
 * <p>{@code /api/export} is the missing half of the systemd audit timer: it
 * serves the real log in the exact NDJSON the independent awk verifier reads, so
 * {@code turnstilectl export | verify-invariants.sh} checks the running system's
 * own log with no shared code. {@code /api/audit} runs the Java twin of that
 * verifier in-process for a quick answer.
 *
 * <p>Both stream. Neither holds the log in memory, so they work at any size; loading
 * the whole log to export it ran out of memory at 600,000 events on a 300 MB heap.
 */
@RestController
@RequestMapping("/api")
public class AuditController {

  private final EventStore store;

  public AuditController(NotifyingEventStore store) {
    this.store = store;
  }

  @GetMapping(value = "/export", produces = "application/x-ndjson")
  public ResponseEntity<StreamingResponseBody> export() {
    StreamingResponseBody body = out -> NdjsonExport.write(store, out);
    return ResponseEntity.ok().contentType(MediaType.parseMediaType("application/x-ndjson")).body(body);
  }

  @GetMapping("/audit")
  public LogAuditor.Report audit() {
    LogAuditor.Session session = new LogAuditor().start();
    store.forEachEvent(stored -> session.accept(NdjsonExport.line(stored).trim()));
    return session.finish();
  }
}
