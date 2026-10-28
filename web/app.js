// Frontend for penny-earnings-map. Fetches the JSON API and renders one collapsible group per
// industry, with the latest two filed quarters side by side and the growth between them.

const state = { stocks: [], summary: null };

const fmtMoney = (v) => {
  if (v === null || v === undefined) return "–";
  const a = Math.abs(v);
  if (a >= 1e9) return (v / 1e9).toFixed(2) + "B";
  if (a >= 1e6) return (v / 1e6).toFixed(1) + "M";
  if (a >= 1e3) return (v / 1e3).toFixed(1) + "K";
  return v.toFixed(0);
};
const fmtPct = (v) => (v === null || v === undefined ? "–" : (v > 0 ? "+" : "") + v.toFixed(1) + "%");
const cls = (v) => (v === null || v === undefined ? "flat" : v > 0 ? "up" : v < 0 ? "down" : "flat");

function el(html) {
  const t = document.createElement("template");
  t.innerHTML = html.trim();
  return t.content.firstElementChild;
}

function tiles() {
  const s = state.summary || {};
  const rows = [
    ["Stocks under $1", s.stocks ?? 0],
    ["Revenue grew", s.grown ?? 0],
    ["Revenue fell", s.shrunk ?? 0],
    ["Priced so far", (s.priced ?? 0) + " / " + (s.universe ?? 0)],
    ["Business areas", Object.keys(s.by_industry || {}).length],
  ];
  document.getElementById("tiles").innerHTML = rows
    .map(([l, n]) => `<div class="tile"><div class="n">${n}</div><div class="l">${l}</div></div>`)
    .join("");
  document.getElementById("status").textContent =
    `${s.stocks ?? 0} under a dollar, ${s.grown ?? 0} growing, ${s.shrunk ?? 0} shrinking`;
  document.getElementById("updated").textContent = "updated " + new Date().toLocaleTimeString();
}

function filtered() {
  const q = document.getElementById("q").value.trim().toLowerCase();
  const dir = document.getElementById("dir").value;
  const sort = document.getElementById("sort").value;
  const onlyData = document.getElementById("onlydata").checked;

  let rows = state.stocks.filter((s) => {
    if (q && !(s.symbol.toLowerCase().includes(q) || s.name.toLowerCase().includes(q))) return false;
    const g = s.revenue_growth;
    if (dir === "up" && !(g > 0)) return false;
    if (dir === "down" && !(g < 0)) return false;
    if (dir === "none" && g !== null && g !== undefined) return false;
    if (onlyData && (g === null || g === undefined)) return false;
    return true;
  });

  const cmp = {
    growth: (a, b) => (b.revenue_growth ?? -1e9) - (a.revenue_growth ?? -1e9),
    price: (a, b) => a.price - b.price,
    symbol: (a, b) => a.symbol.localeCompare(b.symbol),
    name: (a, b) => a.name.localeCompare(b.name),
  }[sort];
  rows.sort(cmp);
  return rows;
}

function render() {
  const rows = filtered();
  const groups = new Map();
  for (const s of rows) {
    const key = s.industry || "Unclassified";
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(s);
  }

  const host = document.getElementById("groups");
  host.innerHTML = "";
  const keys = [...groups.keys()].sort((a, b) => groups.get(b).length - groups.get(a).length);

  for (const key of keys) {
    const list = groups.get(key);
    const grew = list.filter((s) => s.revenue_growth > 0).length;
    const fell = list.filter((s) => s.revenue_growth < 0).length;
    const node = el(`
      <section class="group open">
        <div class="ghead">
          <h2>${key}</h2>
          <span class="count">${list.length}</span>
          <span class="muted">${grew} grew / ${fell} fell</span>
          <span class="grow"></span>
          <span class="muted">click to fold</span>
        </div>
        <div class="gbody"><div class="wrap"><table>
          <tr>
            <th>symbol</th><th>company</th><th>price</th>
            <th>earlier quarter</th><th>revenue</th>
            <th>latest quarter</th><th>revenue</th>
            <th>revenue growth</th><th>net income latest</th><th>income growth</th>
          </tr>
          ${list.map((s) => `
            <tr>
              <td class="sym">${s.symbol}</td>
              <td>${s.name}</td>
              <td class="num">$${s.price.toFixed(4)}</td>
              <td class="muted">${s.q1_label || "–"}</td>
              <td class="num">${fmtMoney(s.q1_revenue)}</td>
              <td class="muted">${s.q2_label || "–"}</td>
              <td class="num">${fmtMoney(s.q2_revenue)}</td>
              <td class="${cls(s.revenue_growth)}">${fmtPct(s.revenue_growth)}</td>
              <td class="num">${fmtMoney(s.q2_income)}</td>
              <td class="${cls(s.income_growth)}">${fmtPct(s.income_growth)}</td>
            </tr>`).join("")}
        </table></div></div>
      </section>`);
    node.querySelector(".ghead").addEventListener("click", () => node.classList.toggle("open"));
    host.appendChild(node);
  }
  document.getElementById("empty").style.display = rows.length ? "none" : "block";
}

async function load() {
  const [summary, stocks] = await Promise.all([
    fetch("/api/summary").then((r) => r.json()),
    fetch("/api/stocks").then((r) => r.json()),
  ]);
  state.summary = summary;
  state.stocks = stocks;
  tiles();
  render();
}

for (const id of ["q", "dir", "sort", "onlydata"]) {
  document.getElementById(id).addEventListener("input", render);
}
load();
setInterval(load, 120000);
