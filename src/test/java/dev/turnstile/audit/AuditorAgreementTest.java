package dev.turnstile.audit;

import static org.assertj.core.api.Assertions.assertThat;

import dev.turnstile.command.SeatCommandHandler;
import dev.turnstile.eventstore.InMemoryEventStore;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Two verifiers, written separately, in different languages, must agree.
 *
 * <p>The awk script is the project's independent audit and the Java
 * {@link LogAuditor} is what the running server uses on itself. If they ever
 * disagreed on a log, one of them would be wrong, and this finds out which kind of
 * log exposes it: random garbage that trips every rule, and hand-built cases for
 * each invariant, all compared line-for-line on the violations they report, not
 * just on pass or fail.
 *
 * <p>The script is run for real through bash. If bash is missing the test fails
 * rather than skipping, because a differential test that silently compares
 * nothing would be worse than none.
 */
class AuditorAgreementTest {

  private static final Path SCRIPT = Path.of("scripts", "verify-invariants.sh").toAbsolutePath();
  private static final Pattern VIOLATION = Pattern.compile("^VIOLATION (\\w+)\\s+line (\\d+):");

  private record Verdict(boolean clean, TreeSet<String> violations) {}

  /** bash reads a Windows backslash as an escape, so hand it forward slashes. */
  private static String posix(Path path) {
    return path.toString().replace('\\', '/');
  }

  /**
   * On Windows a bare "bash" is usually the WSL launcher, which cannot see
   * Windows paths, so Git Bash is looked for explicitly. TURNSTILE_BASH overrides.
   */
  private static String bash() {
    String override = System.getenv("TURNSTILE_BASH");
    if (override != null && !override.isBlank()) {
      return override;
    }
    if (System.getProperty("os.name").toLowerCase().contains("win")) {
      for (String candidate :
          List.of("C:/Program Files/Git/bin/bash.exe", "C:/Program Files (x86)/Git/bin/bash.exe")) {
        if (Files.exists(Path.of(candidate))) {
          return candidate;
        }
      }
      throw new AssertionError(
          "Git Bash not found; install it or set TURNSTILE_BASH. This test refuses to skip.");
    }
    return "bash";
  }

  private static Verdict awkVerdict(Path log) throws Exception {
    Process p =
        new ProcessBuilder(bash(), posix(SCRIPT), "-q", "-f", posix(log))
            .redirectErrorStream(true)
            .start();
    String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(p.waitFor(60, TimeUnit.SECONDS)).as("verifier finished").isTrue();
    int exit = p.exitValue();
    assertThat(exit).as("verifier exit (2 means it could not run): %s", output).isIn(0, 1);

    TreeSet<String> violations = new TreeSet<>();
    for (String line : output.split("\\R")) {
      Matcher m = VIOLATION.matcher(line);
      if (m.find()) {
        violations.add(m.group(1) + "@" + m.group(2));
      }
    }
    return new Verdict(exit == 0, violations);
  }

  private static Verdict javaVerdict(List<String> lines) {
    LogAuditor.Report report = new LogAuditor().audit(lines);
    TreeSet<String> violations = new TreeSet<>();
    Pattern p = Pattern.compile("^(\\w+) line (\\d+):");
    for (String v : report.violations()) {
      Matcher m = p.matcher(v);
      assertThat(m.find()).as(v).isTrue();
      violations.add(m.group(1) + "@" + m.group(2));
    }
    return new Verdict(report.clean(), violations);
  }

  private static String ev(String type, String seat, String hold) {
    return "{\"seq\":1,\"version\":1,\"type\":\"" + type + "\",\"seatId\":\"" + seat
        + "\",\"holdId\":\"" + hold + "\",\"occurredAt\":\"2026-09-21T12:00:00Z\"}";
  }

  private void assertAgree(String why, List<String> lines, Path dir) throws Exception {
    Path file = Files.createTempFile(dir, "log", ".ndjson");
    Files.write(file, (String.join("\n", lines) + "\n").getBytes(StandardCharsets.UTF_8));

    Verdict awk = awkVerdict(file);
    Verdict java = javaVerdict(lines);

    assertThat(java.clean()).as("%s: clean/dirty verdict", why).isEqualTo(awk.clean());
    assertThat(java.violations()).as("%s: which violations, on which lines", why).isEqualTo(awk.violations());
  }

  @Test
  @DisplayName("they agree on each invariant, built by hand")
  void agree_on_each_invariant(@TempDir Path dir) throws Exception {
    assertAgree("clean", List.of(ev("SeatHeld", "s1", "h1"), ev("SeatSold", "s1", "h1")), dir);
    assertAgree(
        "double sale",
        List.of(ev("SeatHeld", "s1", "h1"), ev("SeatSold", "s1", "h1"), ev("SeatSold", "s1", "h1")),
        dir);
    assertAgree("sale with no hold", List.of(ev("SeatSold", "s1", "ghost")), dir);
    assertAgree(
        "activity after sale",
        List.of(ev("SeatHeld", "s1", "h1"), ev("SeatSold", "s1", "h1"), ev("SeatHeld", "s1", "h2")),
        dir);
    assertAgree("unparseable line", List.of("not json at all", ev("SeatHeld", "s1", "h1")), dir);
    assertAgree(
        "ids containing escaped quotes",
        List.of(
            ev("SeatHeld", "s\\\"1", "h\\\"1"),
            ev("SeatSold", "s\\\"1", "h\\\"1"),
            ev("SeatSold", "s\\\"1", "h\\\"1")),
        dir);
    assertAgree(
        "hold on a different seat than the sale",
        List.of(ev("SeatHeld", "s1", "h1"), ev("SeatSold", "s2", "h1")),
        dir);
  }

  @Test
  @DisplayName("they agree on 40 random logs full of violations")
  void agree_on_random_garbage(@TempDir Path dir) throws Exception {
    Random random = new Random(2026);
    String[] types = {"SeatHeld", "HoldReleased", "SeatSold"};
    for (int n = 0; n < 40; n++) {
      List<String> lines = new ArrayList<>();
      int length = 20 + random.nextInt(120);
      for (int i = 0; i < length; i++) {
        lines.add(
            ev(
                types[random.nextInt(types.length)],
                "seat-" + random.nextInt(4),
                "hold-" + random.nextInt(4)));
        if (random.nextInt(40) == 0) {
          lines.add(""); // blank lines still count toward the line numbers reported
        }
      }
      assertAgree("random log #" + n, lines, dir);
    }
  }

  @Test
  @DisplayName("they agree a real simulated sale is clean, and that a tampered copy is not")
  void agree_on_a_real_log_and_a_tampered_one(@TempDir Path dir) throws Exception {
    InMemoryEventStore store = new InMemoryEventStore();
    SeatCommandHandler handler = new SeatCommandHandler(store, Clock.system(ZoneOffset.UTC));
    for (int seat = 0; seat < 30; seat++) {
      for (int buyer = 0; buyer < 3; buyer++) {
        String id = seat + "-" + buyer;
        try {
          handler.hold("seat-" + seat, "h-" + id, "b" + id, Duration.ofMinutes(5), "hold-" + id);
          handler.confirmSale("seat-" + seat, "h-" + id, "o-" + id, "sale-" + id);
        } catch (RuntimeException refused) {
          // The second and third buyers lose, as they should.
        }
      }
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    NdjsonExport.write(store.readAll(), out);
    List<String> lines = new ArrayList<>(Arrays.asList(out.toString(StandardCharsets.UTF_8).split("\n")));

    assertAgree("real log", lines, dir);
    assertThat(javaVerdict(lines).clean()).isTrue();

    String firstSale = lines.stream().filter(l -> l.contains("SeatSold")).findFirst().orElseThrow();
    lines.add(firstSale);
    assertAgree("real log with one sale duplicated", lines, dir);
    assertThat(javaVerdict(lines).clean()).as("the duplicated sale must be caught").isFalse();
  }

  @Test
  void the_export_escapes_what_could_break_the_format() throws IOException {
    InMemoryEventStore store = new InMemoryEventStore();
    SeatCommandHandler handler = new SeatCommandHandler(store, Clock.system(ZoneOffset.UTC));
    handler.hold("seat \"quoted\" \\ back", "h1", "b", Duration.ofMinutes(5), null);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    NdjsonExport.write(store.readAll(), out);

    assertThat(new LogAuditor().audit(List.of(out.toString(StandardCharsets.UTF_8).trim().split("\n"))).events())
        .as("the escaped line still parses as one event")
        .isEqualTo(1);
  }
}
