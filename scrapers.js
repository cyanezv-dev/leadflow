// ── Scrapers de Competencia ───────────────────────────────────
// Cada scraper recibe { brand, medida } y retorna { price, url, in_stock }

const fetch = (...args) => import('node-fetch').then(m => m.default(...args));
const cheerio = require('cheerio');

const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Accept-Language': 'es-CL,es;q=0.9',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
};

async function fetchHtml(url, extraHeaders = {}) {
  const res = await fetch(url, { headers: { ...HEADERS, ...extraHeaders }, timeout: 12000 });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.text();
}

function parsePrice(str) {
  if (!str) return null;
  const num = parseInt(str.replace(/[^\d]/g, ''), 10);
  return isNaN(num) ? null : num;
}

// ── 1. Supermercado del Neumático ─────────────────────────────
async function scrapeSuperneumatico(brand, medida) {
  try {
    const q = encodeURIComponent(`${medida} ${brand}`);
    const url = `https://www.superneumatico.cl/search?q=${q}`;
    const html = await fetchHtml(url);
    const $ = cheerio.load(html);
    let price = null;
    let inStock = false;
    $('[class*="price"], .price, [data-price]').first().each((_, el) => {
      price = parsePrice($(el).text());
    });
    if (!price) {
      const priceText = $('body').text().match(/\$[\s]*[\d\.]+/);
      if (priceText) price = parsePrice(priceText[0]);
    }
    inStock = price !== null;
    return { price, url, in_stock: inStock };
  } catch (e) {
    return { price: null, url: `https://www.superneumatico.cl/search?q=${encodeURIComponent(medida)}`, in_stock: false };
  }
}

// ── 2. ChileNeumatico ─────────────────────────────────────────
async function scrapeChileneumatico(brand, medida) {
  try {
    const q = encodeURIComponent(`${medida} ${brand}`);
    const url = `https://www.chileneumatico.cl/?s=${q}`;
    const html = await fetchHtml(url);
    const $ = cheerio.load(html);
    let price = null;
    $('.woocommerce-Price-amount, .price bdi, ins .woocommerce-Price-amount').first().each((_, el) => {
      price = parsePrice($(el).text());
    });
    const inStock = price !== null;
    return { price, url, in_stock: inStock };
  } catch (e) {
    return { price: null, url: `https://www.chileneumatico.cl/?s=${encodeURIComponent(medida)}`, in_stock: false };
  }
}

// ── 3. Copec ──────────────────────────────────────────────────
async function scrapeCopec(brand, medida) {
  try {
    const q = encodeURIComponent(`${medida} ${brand}`);
    const url = `https://www.copecneumaticos.cl/search?q=${q}`;
    const html = await fetchHtml(url);
    const $ = cheerio.load(html);
    let price = null;
    $('[class*="price"]').first().each((_, el) => {
      const p = parsePrice($(el).text());
      if (p && p > 1000) price = p;
    });
    return { price, url, in_stock: price !== null };
  } catch (e) {
    return { price: null, url: `https://www.copecneumaticos.cl/search?q=${encodeURIComponent(medida)}`, in_stock: false };
  }
}

// ── 4. Dacsa ──────────────────────────────────────────────────
async function scrapeDacsa(brand, medida) {
  try {
    const q = encodeURIComponent(`${medida} ${brand}`);
    const url = `https://www.dacsa.cl/search?type=product&q=${q}`;
    const html = await fetchHtml(url);
    const $ = cheerio.load(html);
    let price = null;
    $('.price, [class*="price"] .money, .product-price').first().each((_, el) => {
      const p = parsePrice($(el).text());
      if (p && p > 1000) price = p;
    });
    return { price, url, in_stock: price !== null };
  } catch (e) {
    return { price: null, url: `https://www.dacsa.cl/search?type=product&q=${encodeURIComponent(medida)}`, in_stock: false };
  }
}

// ── 5. León ───────────────────────────────────────────────────
async function scrapeLeon(brand, medida) {
  try {
    const q = encodeURIComponent(`${medida} ${brand}`);
    const url = `https://www.leon.cl/search?q=${q}`;
    const html = await fetchHtml(url);
    const $ = cheerio.load(html);
    let price = null;
    $('[class*="price"], .woocommerce-Price-amount').first().each((_, el) => {
      const p = parsePrice($(el).text());
      if (p && p > 1000) price = p;
    });
    return { price, url, in_stock: price !== null };
  } catch (e) {
    return { price: null, url: `https://www.leon.cl/search?q=${encodeURIComponent(medida)}`, in_stock: false };
  }
}

// ── 6. Llantas del Pacífico ───────────────────────────────────
async function scrapeLlantasPacifico(brand, medida) {
  try {
    const q = encodeURIComponent(`${medida} ${brand}`);
    const url = `https://www.llantasdelpacifico.cl/search?type=product&q=${q}`;
    const html = await fetchHtml(url);
    const $ = cheerio.load(html);
    let price = null;
    $('[class*="price"] .money, .price').first().each((_, el) => {
      const p = parsePrice($(el).text());
      if (p && p > 1000) price = p;
    });
    return { price, url, in_stock: price !== null };
  } catch (e) {
    return { price: null, url: `https://www.llantasdelpacifico.cl/search?type=product&q=${encodeURIComponent(medida)}`, in_stock: false };
  }
}

const SCRAPERS = {
  'Supermercado del Neumático': scrapeSuperneumatico,
  'ChileNeumatico':             scrapeChileneumatico,
  'Copec':                      scrapeCopec,
  'Dacsa':                      scrapeDacsa,
  'León':                       scrapeLeon,
  'Llantas del Pacífico':       scrapeLlantasPacifico,
};

const COMPETITOR_NAMES = Object.keys(SCRAPERS);

async function scrapeProduct(brand, medida) {
  const results = await Promise.allSettled(
    COMPETITOR_NAMES.map(async (name) => {
      const data = await SCRAPERS[name](brand || '', medida || '');
      return { competitor: name, ...data };
    })
  );
  return results.map((r, i) =>
    r.status === 'fulfilled'
      ? r.value
      : { competitor: COMPETITOR_NAMES[i], price: null, url: null, in_stock: false }
  );
}

module.exports = { scrapeProduct, COMPETITOR_NAMES };
