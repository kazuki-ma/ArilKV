const BENCHMARKING_DATA_URL = "benchmarking/benchmarking-surface.yaml";

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

function nextContentLine(lines, startIndex) {
  for (let index = startIndex + 1; index < lines.length; index += 1) {
    const line = lines[index];
    if (!line.trim() || line.trim().startsWith("#")) continue;
    return line;
  }
  return "";
}

function parseYamlScalar(rawValue) {
  const value = rawValue.trim();
  if (value === "") return "";
  if (value === "true") return true;
  if (value === "false") return false;
  if (/^-?\d+(\.\d+)?$/.test(value)) return Number(value);
  if (value.startsWith("\"") && value.endsWith("\"")) {
    return value.slice(1, -1).replace(/\\"/g, "\"").replace(/\\\\/g, "\\");
  }
  return value;
}

function assignYamlKey(target, key, rawValue, lines, lineIndex, indent) {
  if (rawValue.trim()) {
    target[key] = parseYamlScalar(rawValue);
    return null;
  }

  const nextLine = nextContentLine(lines, lineIndex);
  const nextTrimmed = nextLine.trim();
  const container = nextTrimmed.startsWith("- ") ? [] : {};
  target[key] = container;
  return { indent, value: container };
}

function parseYamlSubset(source) {
  const root = {};
  const stack = [{ indent: -1, value: root }];
  const lines = source.split(/\r?\n/);

  lines.forEach((line, lineIndex) => {
    if (!line.trim() || line.trim().startsWith("#")) return;
    const indent = line.match(/^ */)[0].length;
    const trimmed = line.trim();

    while (stack.length > 1 && indent <= stack[stack.length - 1].indent) {
      stack.pop();
    }

    const parent = stack[stack.length - 1].value;
    if (trimmed.startsWith("- ")) {
      const itemText = trimmed.slice(2);
      if (!Array.isArray(parent)) {
        throw new Error(`invalid YAML sequence at line ${lineIndex + 1}`);
      }

      const keyValue = itemText.match(/^([^:]+):(.*)$/);
      if (keyValue) {
        const item = {};
        parent.push(item);
        const nested = assignYamlKey(
          item,
          keyValue[1].trim(),
          keyValue[2],
          lines,
          lineIndex,
          indent + 2
        );
        stack.push({ indent, value: item });
        if (nested) stack.push(nested);
      } else {
        parent.push(parseYamlScalar(itemText));
      }
      return;
    }

    const keyValue = trimmed.match(/^([^:]+):(.*)$/);
    if (!keyValue || Array.isArray(parent)) {
      throw new Error(`invalid YAML mapping at line ${lineIndex + 1}`);
    }

    const nested = assignYamlKey(
      parent,
      keyValue[1].trim(),
      keyValue[2],
      lines,
      lineIndex,
      indent
    );
    if (nested) stack.push(nested);
  });

  return root;
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
  const productNames = products.map((product) => product.name);
  const featureNames = [];
  products.forEach((product) => {
    (product.claims || []).forEach((claim) => {
      if (!featureNames.includes(claim.feature)) featureNames.push(claim.feature);
    });
  });

  benchmarkingEls.table.replaceChildren();

  const header = document.createElement("div");
  header.className = "surface-row surface-head";
  header.setAttribute("role", "row");
  appendTextElement(header, "span", "Feature area", "columnheader");
  productNames.forEach((product) => {
    appendTextElement(header, "span", product, "columnheader");
  });
  benchmarkingEls.table.appendChild(header);

  featureNames.forEach((feature) => {
    const rowElement = document.createElement("div");
    rowElement.className = "surface-row";
    rowElement.setAttribute("role", "row");
    appendTextElement(rowElement, "span", feature, "cell");
    products.forEach((product) => {
      const claim = (product.claims || []).find((item) => item.feature === feature);
      rowElement.appendChild(renderClaimCell(claim));
    });
    benchmarkingEls.table.appendChild(rowElement);
  });
}

function renderClaimCell(claim) {
  const cell = document.createElement("span");
  cell.setAttribute("role", "cell");
  if (!claim) return cell;

  appendTextElement(cell, "p", claim.summary);
  const references = document.createElement("ul");
  references.className = "claim-references";
  (claim.references || []).forEach((reference) => {
    const item = document.createElement("li");
    const link = document.createElement("a");
    link.href = String(reference.url || "");
    setText(link, reference.label || reference.url || "reference");
    item.appendChild(link);
    if (reference.grep) {
      const grep = document.createElement("code");
      setText(grep, reference.grep);
      item.appendChild(grep);
    }
    references.appendChild(item);
  });
  cell.appendChild(references);
  return cell;
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
  const data = parseYamlSubset(await response.text());
  renderBenchmarkCards(data.overview_cards || []);
  renderSurfaceTable(data);
  renderGuidance(data.guidance || []);
  renderSources((data.products || []).flatMap((product) => (
    (product.claims || []).flatMap((claim) => claim.references || [])
  )));
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
