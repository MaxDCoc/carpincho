# Condenses a raw result file written by run-docker-bench.sh into one Markdown table:
# the mean of the repetitions for every test and pipeline depth, carpincho against
# Redis, plus how much the repetitions disagreed with each other.
#
#   awk -f bench/summarize.awk bench/results/<timestamp>-<mode>.tsv
#
# Input columns (tab separated): server, pipeline, run, test, requests/s, p50 latency (ms).
# Lines starting with # are the run's header and are echoed as a quote block.
BEGIN { FS = "\t" }
/^#/ { sub(/^# ?/, "> "); head = head $0 "\n"; next }
{
  k = $1 SUBSEP $2 SUBSEP $4
  n[k]++; rps[k] += $5; p50[k] += $6
  if (!(k in lo) || $5 < lo[k]) lo[k] = $5
  if (!(k in hi) || $5 > hi[k]) hi[k] = $5
  if (!($2 in seenp)) { seenp[$2] = 1; plist[++np] = $2 }
  if (!($4 in seent)) { seent[$4] = 1; tlist[++nt] = $4 }
}
END {
  printf "%s\n", head
  printf "Requests per second, mean of the repetitions (higher is better). `ratio` = carpincho / Redis.\n\n| test |"
  for (i = 1; i <= np; i++) printf " -P %s carpincho | -P %s Redis | ratio |", plist[i], plist[i]
  printf "\n|---|"
  for (i = 1; i <= np; i++) printf "--:|--:|--:|"
  printf "\n"
  for (j = 1; j <= nt; j++) {
    t = tlist[j]; printf "| %s |", t
    for (i = 1; i <= np; i++) {
      kc = "carpincho" SUBSEP plist[i] SUBSEP t; kr = "redis" SUBSEP plist[i] SUBSEP t
      cm = rps[kc] / n[kc]; rm = rps[kr] / n[kr]
      printf " %d | %d | %.2fx |", cm, rm, (rm > 0 ? cm / rm : 0)
      for (s = 1; s <= 2; s++) {
        k = (s == 1 ? kc : kr); m = rps[k] / n[k]
        if (m > 0 && (hi[k] - lo[k]) / m * 100 > spread) spread = (hi[k] - lo[k]) / m * 100
      }
    }
    printf "\n"
  }
  printf "\nLargest disagreement between repetitions of the same measurement: %.0f%% of its mean.\n", spread
  printf "\np50 latency in ms at -P %s (mean of the repetitions):\n\n| test | carpincho | Redis |\n|---|--:|--:|\n", plist[1]
  for (j = 1; j <= nt; j++) {
    t = tlist[j]
    kc = "carpincho" SUBSEP plist[1] SUBSEP t; kr = "redis" SUBSEP plist[1] SUBSEP t
    printf "| %s | %.3f | %.3f |\n", t, p50[kc] / n[kc], p50[kr] / n[kr]
  }
}
