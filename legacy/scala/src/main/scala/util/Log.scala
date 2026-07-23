package util

/** Two-level logging for the crawler / reader pipeline.
 *
 * The default run should print only what a human needs to see: which data
 * source is being worked, progress, and the final "done / failed" verdict.
 * Everything else — download URLs, per-file "saved N bytes", MOPS step-1/step-2
 * POST traces, per-row "already in DB, skipping" — is diagnostic noise that
 * buries the signal (a single `Main update` printed 6,500+ such lines,
 * 2026-07-16). Those go to `debug`, off unless `QL_VERBOSE=true`.
 *
 * `info` = the human always wants it. `debug` = only when debugging.
 */
object Log {
  val verbose: Boolean = sys.env.get("QL_VERBOSE").exists(v => v == "1" || v.equalsIgnoreCase("true"))

  def info(msg: => String): Unit = println(msg)

  def debug(msg: => String): Unit = if (verbose) println(msg)
}
