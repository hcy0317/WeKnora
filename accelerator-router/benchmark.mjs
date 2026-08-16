const baseURL = (process.env.RERANK_URL || "http://127.0.0.1:18083").replace(/\/$/, "");

const query = "2024年盐亭玉米试验中，正红311在什么密度下亩产最高？";
const documents = [
  "2024年盐亭玉米密度试验采用随机区组设计。正红311在4500株/亩处理下平均亩产650千克，显著高于3800株/亩和5200株/亩处理。",
  "2023年三台县小麦试验比较了不同氮肥用量，最高产量出现在施氮12千克/亩处理。",
  "正红311由绵阳市农业科学研究院参加区域试验，适宜四川丘陵春玉米区种植。",
  "盐亭试验点土壤为紫色土，试验前测定pH为6.8，有机质含量为24克/千克。",
  "2024年梓潼玉米品比试验中，协玉901平均亩产632千克。",
  "密度试验设置3800、4500和5200株/亩三个处理，每个处理重复三次。",
  "项目编号CARS-02-MY，年度任务包括品种筛选、轻简化栽培和示范推广。",
  "2024年盐亭试验结论建议正红311采用4500株/亩，平均亩产650千克。",
  "玉米苗期病虫害调查显示，各处理未出现严重地下害虫危害。",
  "三台试验点降雨较常年偏少，灌溉两次后作物生长恢复正常。",
  "项目年度经费主要用于试验材料、检测分析和示范现场组织。",
  "区试材料应记录播期、密度、施肥、重复、小区面积和收获含水率。",
  "2022年苍溪玉米试验使用4000株/亩处理，但研究对象不是正红311。",
  "专家验收意见要求进一步补充统计分析和原始数据来源说明。",
  "成果清单包括论文两篇、技术规程一项和示范基地五个。",
  "试验数据采用方差分析，并用LSD法进行多重比较，显著性水平为0.05。",
  "推广培训覆盖盐亭、三台和梓潼三个县，共培训技术人员180人次。",
  "正红311在5200株/亩处理下发生轻度倒伏，产量低于4500株/亩处理。",
  "当地对照品种在相同试验条件下平均亩产601千克。",
  "数据来源为2024年盐亭玉米密度试验田间记载表和测产汇总表。",
];

async function run(count) {
  const started = performance.now();
  const response = await fetch(`${baseURL}/v1/rerank`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ model: "adaptive-local-reranker", query, documents: documents.slice(0, count) }),
  });
  const elapsedMs = Math.round((performance.now() - started) * 10) / 10;
  const body = await response.json();
  if (!response.ok) throw new Error(`${response.status}: ${JSON.stringify(body)}`);
  const top = body.results?.[0];
  return {
    count,
    elapsed_ms: elapsedMs,
    backend: response.headers.get("x-weknora-rerank-backend"),
    model: body.model,
    top_index: top?.index,
    top_score: top ? Math.round(top.relevance_score * 10000) / 10000 : null,
  };
}

await run(2); // warm-up
for (const count of [2, 8, 20]) {
  console.log(JSON.stringify(await run(count)));
}
