const FILL_OPACITY = 0.62;

// price -> color, green (cheap) through amber to red (expensive) - normalized per-day against
// the *current* day's own min/max, not a fixed absolute scale, since day-ahead price levels
// swing a lot day to day and a fixed scale would go flat/uninformative on calm days.
const PRICE_LOW = [86, 156, 92];
const PRICE_MID = [214, 168, 62];
const PRICE_HIGH = [190, 66, 66];

// only zones actually priced in EUR feed the price-intensity scale. in practice that's every
// SDAC/SEM_DA zone (including CH and the Nordics, whose day-ahead auction clears in EUR even
// though their retail currency isn't) - only GB (its own N2EX/GbHalfHour auctions, not SDAC)
// lands in GBP. there's no FX conversion anywhere in this repo (see project-overview.md), so
// mixing a non-EUR price into the same 0-1 scale as EUR zones would silently compare unrelated
// units instead of just excluding the rare zone that isn't on this scale.
const SCALE_CURRENCY = "EUR";

// display names for the hover card only - map/API both key everything by the plain
// bidding_zone code (see monitoring/zone_map/zones.py IN_SCOPE_ZONES).
const ZONE_NAMES = {
  AT: "Austria", BE: "Belgium", BG: "Bulgaria", CH: "Switzerland", CZ: "Czech Republic",
  DE: "Germany", DK1: "Denmark (West)", DK2: "Denmark (East)", EE: "Estonia", ES: "Spain",
  FI: "Finland", FR: "France", GB: "Great Britain", GR: "Greece", HR: "Croatia",
  HU: "Hungary", IE: "Ireland",
  IT_NORD: "Italy (North)", IT_CNOR: "Italy (Center-North)", IT_CSUD: "Italy (Center-South)",
  IT_SUD: "Italy (South)", IT_SICI: "Italy (Sicily)", IT_SARD: "Italy (Sardinia)", IT_CALA: "Italy (Calabria)",
  LT: "Lithuania", LV: "Latvia", NL: "Netherlands",
  NO1: "Norway 1", NO2: "Norway 2", NO3: "Norway 3", NO4: "Norway 4", NO5: "Norway 5",
  PL: "Poland", PT: "Portugal", RO: "Romania",
  SE1: "Sweden 1", SE2: "Sweden 2", SE3: "Sweden 3", SE4: "Sweden 4",
  SI: "Slovenia", SK: "Slovakia",
};

let map;
const zoneLayers = new Map(); // bidding_zone -> Leaflet layer, built once
let hoverTooltip = null;
let closeTimer = null;

// hovering the tooltip itself counts as "still hovering the zone" - without this, moving the
// mouse from the shape onto the card fires the layer's mouseout and the card vanishes before
// you can actually reach it.
function cancelClose() {
  if (closeTimer) {
    clearTimeout(closeTimer);
    closeTimer = null;
  }
}

function scheduleClose() {
  cancelClose();
  closeTimer = setTimeout(() => {
    if (hoverTooltip) {
      map.removeLayer(hoverTooltip);
      hoverTooltip = null;
    }
  }, 150);
}

function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

// EUR-only min/max for the day currently on screen, recomputed on every load/date change.
let priceRange = { min: 0, max: 0 };

function computePriceRange(priceByZone) {
  const prices = Object.values(priceByZone)
    .filter((z) => z.has_data && z.currency === SCALE_CURRENCY)
    .map((z) => z.avg_price);
  priceRange = prices.length ? { min: Math.min(...prices), max: Math.max(...prices) } : { min: 0, max: 0 };
}

function lerp(a, b, t) {
  return a + (b - a) * t;
}

function priceToColor(price) {
  const span = priceRange.max - priceRange.min;
  const t = span > 0 ? Math.min(1, Math.max(0, (price - priceRange.min) / span)) : 0.5;
  const [c1, c2, localT] = t <= 0.5 ? [PRICE_LOW, PRICE_MID, t / 0.5] : [PRICE_MID, PRICE_HIGH, (t - 0.5) / 0.5];
  const rgb = c1.map((v, i) => Math.round(lerp(v, c2[i], localT)));
  return `rgb(${rgb.join(",")})`;
}

function zoneStyle(info) {
  if (info && info.has_data && info.currency === SCALE_CURRENCY) {
    const fill = priceToColor(info.avg_price);
    return { fillColor: fill, fillOpacity: FILL_OPACITY, color: fill, weight: 1 };
  }
  if (info && info.has_data) {
    // priced, but in a currency not on the EUR scale above - a deliberately distinct (not
    // green/amber/red, not the pending off-white) treatment so it doesn't get misread as
    // either "cheap" or "no data".
    return { fillColor: cssVar("--noneur-fill"), fillOpacity: 0.85, color: cssVar("--noneur-stroke"), weight: 1 };
  }
  return {
    fillColor: cssVar("--nodata-fill"), fillOpacity: 1,
    color: cssVar("--nodead-stroke"), weight: 1, dashArray: "3,3",
  };
}

function formatPrice(info) {
  if (!info || !info.has_data) return null;
  return `${info.avg_price.toFixed(1)} ${info.currency}/MWh`;
}

function zoneLabelHtml(zoneCode, info) {
  const price = formatPrice(info);
  return `<div class="zone-chip"><span class="zone-code">${zoneCode}</span>${price ? `<span class="price">${price}</span>` : ""}</div>`;
}

function sourceBreakdownHtml(info) {
  return info.sources
    .map((s) => {
      const complete = s.actual >= s.expected;
      const dotColor = complete ? "#0ca30c" : "#fab219";
      return `<tr>
        <td><span class="status-dot" style="background:${dotColor}"></span>${s.source} (${s.market})</td>
        <td>${s.actual}/${s.expected}</td>
        <td>${s.avg_price.toFixed(2)} ${info.currency}</td>
      </tr>`;
    })
    .join("");
}

function curveChartHtml(info) {
  if (!info.curve.length) return "";
  const W = 216, H = 56, PAD = 3;
  const prices = info.curve.map((p) => p.price);
  const lo = Math.min(...prices), hi = Math.max(...prices);
  const span = hi - lo || 1;
  const stepX = (W - PAD * 2) / (info.curve.length - 1);
  const points = info.curve.map((p, i) => [
    PAD + i * stepX,
    PAD + (H - PAD * 2) * (1 - (p.price - lo) / span),
  ]);

  const line = points.map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  const area = `${line} L${points[points.length - 1][0].toFixed(1)},${H - PAD} L${points[0][0].toFixed(1)},${H - PAD} Z`;

  let zeroLine = "";
  if (lo < 0 && hi > 0) {
    const zy = PAD + (H - PAD * 2) * (1 - (0 - lo) / span);
    zeroLine = `<line x1="${PAD}" y1="${zy.toFixed(1)}" x2="${W - PAD}" y2="${zy.toFixed(1)}" class="chart-zero" />`;
  }

  const maxIdx = prices.indexOf(hi);
  const minIdx = prices.indexOf(lo);
  const dot = ([x, y]) => `<circle class="chart-dot" cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="2.2" />`;

  return `
    <div class="curve-heading">Baseload curve &mdash; ${info.curve_source}</div>
    <svg class="curve-chart" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none">
      <defs>
        <linearGradient id="fill-${info.curve_source.replace(/\W/g, "")}" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" class="chart-fill-a" />
          <stop offset="100%" class="chart-fill-b" />
        </linearGradient>
      </defs>
      ${zeroLine}
      <path d="${area}" fill="url(#fill-${info.curve_source.replace(/\W/g, "")})" />
      <path d="${line}" class="chart-line" />
      ${dot(points[maxIdx])}${dot(points[minIdx])}
    </svg>
    <div class="chart-minmax">
      <span>${info.curve[minIdx].time} &middot; ${lo.toFixed(1)} ${info.currency}</span>
      <span>${info.curve[maxIdx].time} &middot; ${hi.toFixed(1)} ${info.currency}</span>
    </div>
  `;
}

function tooltipHtml(zoneCode, info, expanded) {
  const name = ZONE_NAMES[zoneCode] || "";
  const expandBtn = `<button class="expand-btn" aria-label="${expanded ? "Collapse" : "Expand"}" title="${expanded ? "Collapse" : "Expand"}">${expanded ? "&#10529;" : "&#10530;"}</button>`;
  const title = `<div class="zone-title">${zoneCode}<span class="zone-name">${name}</span>${expandBtn}</div>`;
  if (!info || !info.has_data) {
    return `${title}<div class="empty-note">no DAY_AHEAD data yet</div>`;
  }
  const headlineColor = info.currency === SCALE_CURRENCY ? priceToColor(info.avg_price) : cssVar("--noneur-stroke");
  return `
    ${title}
    <div class="headline" style="color:${headlineColor}">${formatPrice(info)}<span class="headline-label">baseload</span></div>
    <table>${sourceBreakdownHtml(info)}</table>
    ${curveChartHtml(info)}
  `;
}

function updateZone(zoneCode, layer, info) {
  layer._priceInfo = info; // read by the mouseover handler below, always the latest fetch
  layer.setStyle(zoneStyle(info));
  layer.setTooltipContent(zoneLabelHtml(zoneCode, info));
}

function applyPrices(priceByZone) {
  computePriceRange(priceByZone);
  updateScaleLegend();
  for (const [zoneCode, layer] of zoneLayers) {
    updateZone(zoneCode, layer, priceByZone[zoneCode]);
  }
}

function updateScaleLegend() {
  const min = document.getElementById("scale-min");
  const max = document.getElementById("scale-max");
  if (!min || !max) return;
  const hasRange = priceRange.max > priceRange.min || (priceRange.min !== 0 && priceRange.max !== 0);
  min.textContent = hasRange ? `${priceRange.min.toFixed(0)}` : "–";
  max.textContent = hasRange ? `${priceRange.max.toFixed(0)} ${SCALE_CURRENCY}/MWh` : "–";
}

function shiftDate(dateStr, days) {
  const d = new Date(`${dateStr}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

async function loadPrices(dateStr) {
  const url = dateStr ? `/api/prices?date=${dateStr}` : "/api/prices";
  const prices = await fetch(url).then((r) => r.json());
  document.getElementById("date-input").value = prices.date;
  applyPrices(prices.zones);
}

async function main() {
  const [contextGeo, gridGeo, zonesGeo, prices] = await Promise.all([
    fetch("/static/geo/context.geojson").then((r) => r.json()),
    fetch("/static/geo/grid.geojson").then((r) => r.json()),
    fetch("/static/geo/zones.geojson").then((r) => r.json()),
    fetch("/api/prices").then((r) => r.json()),
  ]);

  const priceByZone = prices.zones;
  document.getElementById("date-input").value = prices.date;
  computePriceRange(priceByZone);
  updateScaleLegend();

  // zoomSnap/zoomDelta below 1 let the map rest at quarter zoom levels instead of only whole
  // integers - Leaflet's default (zoomSnap: 1) is what makes wheel-zoom feel stepped/jumpy,
  // since every scroll tick has to commit to a full level; a smaller snap lets it ease in
  // continuously instead.
  map = L.map("map", {
    attributionControl: true, zoomControl: true, worldCopyJump: false, maxBoundsViscosity: 1.0,
    zoomSnap: 0.25, zoomDelta: 0.5, wheelPxPerZoomLevel: 100,
  });
  map.attributionControl.setPrefix(false);

  L.geoJSON(contextGeo, {
    interactive: false,
    attribution: "Natural Earth",
    style: () => ({ fillColor: cssVar("--context-fill"), fillOpacity: 1, color: cssVar("--context-stroke"), weight: 1 }),
  }).addTo(map);

  // faint, purely decorative - not meant to be noticed at a glance (see project-overview.md).
  // real attribution matters here though: ODbL requires it for the produced map, not just the
  // license name in a code comment.
  L.geoJSON(gridGeo, {
    interactive: false,
    attribution: "Grid: &copy; OpenStreetMap contributors, via GridKit (ODbL)",
    style: () => ({ color: cssVar("--grid-line"), weight: 0.6, opacity: 1 }),
  }).addTo(map);

  const zonesLayer = L.geoJSON(zonesGeo, {
    attribution: "Zones: EnergieID/entsoe-py",
    style: (feature) => zoneStyle(priceByZone[feature.properties.bidding_zone]),
    onEachFeature: (feature, layer) => {
      const zoneCode = feature.properties.bidding_zone;
      layer._priceInfo = priceByZone[zoneCode];
      zoneLayers.set(zoneCode, layer);

      layer.bindTooltip(zoneLabelHtml(zoneCode, layer._priceInfo), {
        permanent: true, direction: "center", className: "zone-label", interactive: false,
      });

      // a layer can have exactly one *bound* tooltip, so the permanent zone-code label uses
      // bindTooltip while the hover source-breakdown is a separate unbound L.tooltip we
      // add/move/remove by hand - two bindTooltip calls on the same layer would just replace
      // each other instead of coexisting.
      layer.on("mouseover", () => {
        cancelClose();
        layer.setStyle({ weight: 2 });
        if (hoverTooltip) map.removeLayer(hoverTooltip);

        let expanded = false;

        // anchored at the shape's center, not the cursor - a tooltip that chases the mouse
        // can never be clicked into (moving toward it just keeps moving it away).
        hoverTooltip = L.tooltip(layer.getBounds().getCenter(), {
          className: "source-tooltip", direction: "top", offset: [0, -10], interactive: true,
        })
          .setContent(tooltipHtml(zoneCode, layer._priceInfo, expanded))
          .addTo(map);

        const el = hoverTooltip.getElement();
        if (el) {
          // interactive:true stops mouse/wheel events from passing through to the map (so the
          // expand button is actually clickable), which otherwise also lets the map itself
          // absorb the wheel event as a zoom - disableScrollPropagation stops that.
          L.DomEvent.disableScrollPropagation(el);
          L.DomEvent.disableClickPropagation(el);
          el.addEventListener("mouseenter", cancelClose);
          el.addEventListener("mouseleave", scheduleClose);

          // setContent() replaces the button along with the rest of the markup, so the click
          // listener needs rebinding after every toggle, not just once.
          const bindExpandButton = () => {
            const btn = el.querySelector(".expand-btn");
            if (!btn) return;
            btn.addEventListener("click", (e) => {
              e.stopPropagation();
              expanded = !expanded;
              el.classList.toggle("expanded", expanded);
              // setContent() triggers Leaflet's own reposition logic based on the new size, so
              // the card grows/shrinks in place instead of drifting off its anchor.
              hoverTooltip.setContent(tooltipHtml(zoneCode, layer._priceInfo, expanded));
              bindExpandButton();
            });
          };
          bindExpandButton();
        }
      });
      layer.on("mouseout", () => {
        layer.setStyle({ weight: 1 });
        scheduleClose();
      });
    },
  }).addTo(map);

  const europeBounds = zonesLayer.getBounds();
  map.fitBounds(europeBounds, { padding: [16, 16] });

  // lock the camera to "all of Europe" as the widest view and a generously padded version of
  // the same box as the pan limit. context.geojson itself covers the whole world (so panning
  // shows real grey landmass, not empty background, if these limits are ever loosened) - this
  // restriction is purely about what's useful to look at, not a workaround for missing data.
  map.setMinZoom(map.getZoom());
  map.setMaxZoom(map.getZoom() + 6);
  map.setMaxBounds(europeBounds.pad(0.25));
  window.addEventListener("resize", () => map.invalidateSize());

  const dateInput = document.getElementById("date-input");
  document.getElementById("prev-day").addEventListener("click", () => loadPrices(shiftDate(dateInput.value, -1)));
  document.getElementById("next-day").addEventListener("click", () => loadPrices(shiftDate(dateInput.value, 1)));
  dateInput.addEventListener("change", () => loadPrices(dateInput.value));

  const loader = document.getElementById("loader");
  if (loader) {
    loader.classList.add("hidden");
    setTimeout(() => loader.remove(), 300);
  }
}

main();
