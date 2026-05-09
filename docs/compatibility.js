const DATA_URL = "compatibility/compatibility-data.yaml";

const els = {
  commandFilter: document.getElementById("command-filter"),
  statusFilter: document.getElementById("status-filter"),
  textFilter: document.getElementById("text-filter"),
  copyLink: document.getElementById("copy-link"),
  summary: document.getElementById("suite-summary"),
  resultCount: document.getElementById("result-count"),
  suiteResults: document.getElementById("suite-results"),
  commandCount: document.getElementById("command-count"),
  commandIndex: document.getElementById("command-index")
};

let state = {
  data: null,
  regexes: [],
  regexError: ""
};

function parseScalar(value) {
  const text = value.trim();
  if (text === "true") return true;
  if (text === "false") return false;
  if (/^-?\d+(\.\d+)?$/.test(text)) return Number(text);
  if (text.startsWith("[") && text.endsWith("]")) {
    const inner = text.slice(1, -1).trim();
    if (!inner) return [];
    return inner.split(/,\s*/).map(parseScalar);
  }
  if (text.startsWith("\"") && text.endsWith("\"")) {
    return text.slice(1, -1).replace(/\\"/g, "\"").replace(/\\\\/g, "\\");
  }
  return text;
}

function parseCompatibilityYaml(source) {
  const root = { summary: {}, commands: [], test_cases: [] };
  let section = "";
  let current = null;

  source.split(/\r?\n/).forEach((line) => {
    if (!line.trim() || line.trim().startsWith("#")) return;
    if (!line.startsWith(" ")) {
      const [key, ...rest] = line.split(":");
      section = key.trim();
      current = null;
      if (rest.join(":").trim()) root[section] = parseScalar(rest.join(":"));
      return;
    }

    if (section === "summary") {
      const trimmed = line.trim();
      const [key, ...rest] = trimmed.split(":");
      root.summary[key.trim()] = parseScalar(rest.join(":"));
      return;
    }

    if (section === "commands" || section === "test_cases") {
      const trimmed = line.trim();
      if (trimmed.startsWith("- ")) {
        current = {};
        root[section].push(current);
        const first = trimmed.slice(2);
        const [key, ...rest] = first.split(":");
        current[key.trim()] = parseScalar(rest.join(":"));
        return;
      }
      if (current) {
        const [key, ...rest] = trimmed.split(":");
        current[key.trim()] = parseScalar(rest.join(":"));
      }
    }
  });

  return root;
}

function readQuery() {
  const params = new URLSearchParams(window.location.search);
  els.commandFilter.value = params.get("commands") || "";
  els.statusFilter.value = params.get("status") || "";
  els.textFilter.value = params.get("q") || "";
}

function writeQuery() {
  const params = new URLSearchParams();
  if (els.commandFilter.value.trim()) params.set("commands", els.commandFilter.value.trim());
  if (els.statusFilter.value) params.set("status", els.statusFilter.value);
  if (els.textFilter.value.trim()) params.set("q", els.textFilter.value.trim());
  const query = params.toString();
  const next = `${window.location.pathname}${query ? `?${query}` : ""}`;
  window.history.replaceState(null, "", next);
}

function compileRegexes(input) {
  const parts = input
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
  const regexes = [];

  for (const part of parts) {
    try {
      regexes.push(new RegExp(part, "i"));
    } catch (error) {
      return { regexes: [], error: error.message };
    }
  }
  return { regexes, error: "" };
}

function commandMatches(command) {
  if (!state.regexes.length) return true;
  return state.regexes.some((regex) => regex.test(command));
}

function caseMatches(testCase) {
  const status = els.statusFilter.value;
  const text = els.textFilter.value.trim().toLowerCase();
  const commands = testCase.commands || [];
  const commandHit = !state.regexes.length || commands.some(commandMatches);
  const statusHit = !status || testCase.status === status;
  const haystack = `${testCase.case} ${testCase.unit} ${testCase.details} ${commands.join(" ")}`.toLowerCase();
  const textHit = !text || haystack.includes(text);
  return commandHit && statusHit && textHit;
}

function commandSurfaceMatches(command) {
  const text = els.textFilter.value.trim().toLowerCase();
  const commandHit = commandMatches(command.command);
  const haystack = `${command.command} ${command.declaration_status} ${command.implementation_status} ${command.comment}`.toLowerCase();
  const textHit = !text || haystack.includes(text);
  return commandHit && textHit;
}

function renderSummary(data) {
  const summary = data.summary || {};
  els.summary.innerHTML = `
    <dl>
      <div><dt>Suite cases</dt><dd>${summary.test_case_count || 0}</dd></div>
      <div><dt>Passing</dt><dd>${summary.test_pass_count || 0}</dd></div>
      <div><dt>Declared commands</dt><dd>${summary.arilkv_declared_count || 0}</dd></div>
      <div><dt>Full maturity</dt><dd>${summary.full_count || 0}</dd></div>
    </dl>
  `;
}

function badgeClass(value) {
  return String(value || "").toLowerCase().replace(/[^a-z0-9]+/g, "-");
}

function renderSuiteCases(cases) {
  els.resultCount.textContent = `${cases.length} case${cases.length === 1 ? "" : "s"}`;
  els.suiteResults.innerHTML = cases.map((testCase) => {
    const commands = (testCase.commands || []).map((command) => (
      `<button class="command-chip" type="button" data-command="${command}">${command}</button>`
    )).join("");
    return `
      <article class="suite-case" id="case-${testCase.case}">
        <div>
          <span class="status-badge ${badgeClass(testCase.status)}">${testCase.status}</span>
          <h3>${testCase.case}</h3>
          <p>${testCase.details || ""}</p>
        </div>
        <div class="case-meta">
          <span>${testCase.unit || "suite"}</span>
          <div>${commands}</div>
        </div>
      </article>
    `;
  }).join("") || `<p class="empty-state">No suite cases match the current filters.</p>`;
}

function renderCommandIndex(commands) {
  els.commandCount.textContent = `${commands.length} command${commands.length === 1 ? "" : "s"}`;
  els.commandIndex.innerHTML = commands.map((command) => `
    <article>
      <button class="command-name" type="button" data-command="^${command.command}$">${command.command}</button>
      <span class="status-badge ${badgeClass(command.implementation_status || command.declaration_status)}">${command.implementation_status || command.declaration_status}</span>
      <p>${command.comment || "Declared command surface."}</p>
    </article>
  `).join("") || `<p class="empty-state">No commands match the current filters.</p>`;
}

function render() {
  const data = state.data;
  if (!data) return;
  const compiled = compileRegexes(els.commandFilter.value);
  state.regexes = compiled.regexes;
  state.regexError = compiled.error;
  writeQuery();

  if (state.regexError) {
    els.resultCount.textContent = "Invalid regex";
    els.suiteResults.innerHTML = `<p class="empty-state">Invalid command regex: ${state.regexError}</p>`;
    return;
  }

  renderSuiteCases((data.test_cases || []).filter(caseMatches));
  renderCommandIndex((data.commands || []).filter(commandSurfaceMatches));
}

function setCommandFilter(value) {
  els.commandFilter.value = value;
  render();
}

async function loadData() {
  readQuery();
  const response = await fetch(DATA_URL, { cache: "no-cache" });
  if (!response.ok) throw new Error(`failed to load ${DATA_URL}: ${response.status}`);
  state.data = parseCompatibilityYaml(await response.text());
  renderSummary(state.data);
  render();
}

[els.commandFilter, els.statusFilter, els.textFilter].forEach((el) => {
  el.addEventListener("input", render);
  el.addEventListener("change", render);
});

document.addEventListener("click", async (event) => {
  const chip = event.target.closest("[data-command]");
  if (chip) {
    setCommandFilter(chip.dataset.command);
    return;
  }

  if (event.target === els.copyLink) {
    await navigator.clipboard.writeText(window.location.href);
    els.copyLink.textContent = "Copied";
    window.setTimeout(() => {
      els.copyLink.textContent = "Copy link";
    }, 1200);
  }
});

loadData().catch((error) => {
  els.summary.innerHTML = `<span>${error.message}</span>`;
  els.suiteResults.innerHTML = `<p class="empty-state">${error.message}</p>`;
});
