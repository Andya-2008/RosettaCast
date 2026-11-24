// Exercises the real glossaryFor() from the deployed module.
//   node glossary.test.mjs
import { glossaryFor } from "./onSentenceCreated.js";

const cases = [
  ["그러고는 저에게 와드 보조 서기 부름을 제안하셨습니다.", "ko", ["en", "es"], ["와드", "부름"]],
  ["감독님은 성약의 길에 대해 말씀하셨습니다.", "ko", ["en"], ["성약의 길", "감독"]],
  ["한때 성역자가 아버지를 방문하기 시작했습니다.", "ko", ["en", "es"], ["성역자"]],
  ["그 아버지는 성전 봉사자가 되기로 결심하셨습니다.", "ko", ["en"], ["성전 봉사자"]],
  ["That was awkward, and we walked toward the door.", null, ["ko"], []],
  ["The ward council met after sacrament meeting.", null, ["ko"], ["ward"]],
  ["오늘은 날씨가 좋습니다.", "ko", ["en"], []],
];

let failures = 0;
for (const [text, src, targets, expect] of cases) {
  const hits = glossaryFor(text, src, targets);
  const terms = hits.map((h) => h.term);
  const missing = expect.filter((e) => !terms.includes(e));
  const ok = missing.length === 0;
  if (!ok) failures++;
  console.log(`${ok ? "ok  " : "FAIL"} ${text.slice(0, 44)}`);
  for (const h of hits) console.log(`       ${h.line}`);
  if (missing.length) console.log(`       expected but missing: ${missing.join(", ")}`);
  if (!hits.length) console.log("       (no glossary terms — correct if none apply)");
}
console.log(failures ? `\n${failures} case(s) failed` : "\nall cases passed");
process.exit(failures ? 1 : 0);
