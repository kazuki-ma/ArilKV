const BENCHMARKING_DATA_URL = "benchmarking/benchmarking-surface.json";

const benchmarkingEls = {
  cards: document.getElementById("benchmark-cards"),
  table: document.getElementById("surface-table"),
  guidance: document.getElementById("benchmark-guidance"),
  sources: document.getElementById("benchmark-sources")
};

function setText(element, value) {
  element.innerText = String(value || "");
  return element;
}

function appendTextElement(parent, tagName, value, role) {
  const element = document.createElement(tagName);
  if (role) element.setAttribute("role", role);
  setText(element, value);
  parent.appendChild(element);
  return element;
}

function renderBenchmarkCards(cards) {
  benchmarkingEls.cards.replaceChildren();
  cards.forEach((card) => {
    const article = document.createElement("article");
    appendTextElement(article, "h3", card.title);
    appendTextElement(article, "p", card.body);
    benchmarkingEls.cards.appendChild(article);
  });
}

function renderSurfaceTable(data) {
  const products = data.products || [];
  benchmarkingEls.table.replaceChildren();

  const header = document.createElement("div");
  header.className = "surface-row surface-head";
  header.setAttribute("role", "row");
  appendTextElement(header, "span", "Feature area", "columnheader");
  products.forEach((product) => {
    appendTextElement(header, "span", product, "columnheader");
  });
  benchmarkingEls.table.appendChild(header);

  (data.surface_rows || []).forEach((row) => {
    const rowElement = document.createElement("div");
    rowElement.className = "surface-row";
    rowElement.setAttribute("role", "row");
    appendTextElement(rowElement, "span", row.feature, "cell");
    products.forEach((product) => {
      appendTextElement(rowElement, "span", (row.cells || {})[product] || "", "cell");
    });
    benchmarkingEls.table.appendChild(rowElement);
  });
}

function renderGuidance(items) {
  benchmarkingEls.guidance.replaceChildren();
  items.forEach((item) => {
    appendTextElement(benchmarkingEls.guidance, "li", item);
  });
}

function renderSources(sources) {
  benchmarkingEls.sources.replaceChildren();
  sources.forEach((source) => {
    const item = document.createElement("li");
    const link = document.createElement("a");
    link.href = String(source.url || "");
    setText(link, source.label);
    const note = document.createElement("span");
    setText(note, source.note);
    item.appendChild(link);
    item.appendChild(note);
    benchmarkingEls.sources.appendChild(item);
  });
}

async function loadBenchmarkingData() {
  const response = await fetch(BENCHMARKING_DATA_URL, { cache: "no-cache" });
  if (!response.ok) {
    throw new Error(`failed to load ${BENCHMARKING_DATA_URL}: ${response.status}`);
  }
  const data = await response.json();
  renderBenchmarkCards(data.overview_cards || []);
  renderSurfaceTable(data);
  renderGuidance(data.guidance || []);
  renderSources(data.sources || []);
}

loadBenchmarkingData().catch((error) => {
  const cardError = document.createElement("p");
  cardError.className = "empty-state";
  setText(cardError, error.message);
  const tableError = cardError.cloneNode(true);
  const guidanceError = document.createElement("li");
  setText(guidanceError, error.message);
  const sourceError = guidanceError.cloneNode(true);
  benchmarkingEls.cards.replaceChildren(cardError);
  benchmarkingEls.table.replaceChildren(tableError);
  benchmarkingEls.guidance.replaceChildren(guidanceError);
  benchmarkingEls.sources.replaceChildren(sourceError);
});
