package dev.turnstile.web;

import java.util.regex.Pattern;

/**
 * Identifiers arrive from URLs, headers and JSON bodies and end up in the log, in
 * NDJSON exports and in log lines. Restricting them up front keeps a hostile id
 * from becoming a broken export or a forged log entry.
 */
final class Ids {

  private static final Pattern SAFE = Pattern.compile("[A-Za-z0-9._:@-]{1,100}");

  private Ids() {}

  static String require(String field, String value) {
    if (value == null || !SAFE.matcher(value).matches()) {
      throw new IllegalArgumentException(
          field + " must be 1-100 characters from A-Z a-z 0-9 . _ : @ -");
    }
    return value;
  }
}
