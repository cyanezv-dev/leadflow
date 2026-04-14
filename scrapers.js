// ── Scrapers de Competencia ───────────────────────────────────
const fetch = (...args) => import('node-fetch').then(m => m.default(...args));
const cheerio = require('cheerio');
const puppeteer = require('puppeteer-core');

const CHROME_PATH = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';

// Lanzar navegador headless (singleton reutilizable)
let browser = null;
async function getBrowser() {
  if (browser) {
    try { await browser.version(); return browser; } catch(_) { browser = null; }
  }
  browser = await puppeteer.launch({
    executablePath: CHROME_PATH,
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
  });
  return browser;
}

async function fetchHtmlWithBrowser(url) {
  const b = await getBrowser();
  const page = await b.newPage();
  try {
    await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
    await page.setExtraHTTPHeaders({ 'Accept-Language': 'es-CL,es;q=0.9' });
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 20000 });
    // Esperar un poco más para que React renderice
    await new Promise(r => setTimeout(r, 1500));
    return await page.content();
  } finally {
    await page.close();
  }
}

const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Accept-Language': 'es-CL,es;q=0.9',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
};

const COMPETITOR_NAMES = [
  'Supermercado del Neumático',
  'ChileNeumatico',
  'Copec',
  'Dacsa',
  'León',
  'Llantas del Pacífico',
];

async function fetchHtml(url) {
  const res = await fetch(url, { headers: HEADERS, timeout: 15000 });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.text();
}

function parsePrice(str) {
  if (!str) return null;
  // Remove currency symbols, dots as thousands separator, keep digits
  const clean = str.replace(/[^\d,]/g, '').replace(',', '.');
  const num = parseInt(str.replace(/[^\d]/g, ''), 10);
  return isNaN(num) || num < 500 ? null : num;
}

// Extraer precio de un HTML usando un selector CSS configurable
function extractPrice($, selector) {
  if (!selector) return null;
  const selectors = selector.split(',').map(s => s.trim());
  for (const sel of selectors) {
    let price = null;
    $(sel).each((_, el) => {
      if (price) return;
      const text = $(el).text().trim();
      const p = parsePrice(text);
      if (p && p > 500 && p < 100000000) price = p;
    });
    if (price) return price;
  }
  return null;
}

// Detectar si el HTML es una SPA vacía (React/Vue/Angular)
function isSPA(html) {
  const $ = cheerio.load(html);
  const rootText = $('#root').text().trim();
  const appText  = $('#app').text().trim();
  return (html.includes('id="root"') && rootText.length < 50) ||
         (html.includes('id="app"')  && appText.length  < 50);
}

// Scrape con configuración dinámica (URL + selector CSS)
async function scrapeWithConfig({ url, price_selector, link_selector }) {
  const startMs = Date.now();
  try {
    let html = await fetchHtml(url);
    let usedPuppeteer = false;

    // Si es SPA, usar Puppeteer para renderizar JS
    if (isSPA(html)) {
      usedPuppeteer = true;
      html = await fetchHtmlWithBrowser(url);
    }

    const $ = cheerio.load(html);
    const price = extractPrice($, price_selector);

    // Extraer URL del primer resultado si hay selector de link
    let productUrl = url;
    if (link_selector) {
      const href = $(link_selector).first().attr('href');
      if (href) productUrl = href.startsWith('http') ? href : new URL(href, url).href;
    }

    // Mostrar contexto del precio para depuración
    const priceContext = price_selector
      ? $(price_selector.split(',')[0].trim()).first().text().trim().slice(0, 80)
      : '';

    const isBlocked = /cloudflare|captcha|robot|access denied|403 forbidden|just a moment|checking your browser/i.test(html);
    const isRedirect = html.length < 2000 && /<meta[^>]*refresh|window\.location/i.test(html);
    const htmlSnippet = html.slice(0, 800).replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();

    return {
      price,
      url: productUrl,
      in_stock: price !== null,
      ms: Date.now() - startMs,
      price_context: priceContext,
      html_length: html.length,
      html_snippet: htmlSnippet,
      is_blocked: isBlocked,
      is_redirect: isRedirect,
      used_puppeteer: usedPuppeteer,
      error: null,
    };
  } catch (e) {
    return {
      price: null,
      url,
      in_stock: false,
      ms: Date.now() - startMs,
      error: e.message,
    };
  }
}

// Scrape todos los competidores para un producto usando configs de BD
async function scrapeProduct(brand, medida, configs = []) {
  const query = `${medida || ''} ${brand || ''}`.trim();

  const results = await Promise.allSettled(
    COMPETITOR_NAMES.map(async (name) => {
      const cfg = configs.find(c => c.competitor === name);
      if (!cfg || !cfg.active || !cfg.search_url) {
        return { competitor: name, price: null, url: cfg?.search_url || null, in_stock: false };
      }
      const url = cfg.search_url.replace('{query}', encodeURIComponent(query));
      const result = await scrapeWithConfig({
        url,
        price_selector: cfg.price_selector,
        link_selector: cfg.link_selector,
      });
      return { competitor: name, ...result };
    })
  );

  return results.map((r, i) =>
    r.status === 'fulfilled'
      ? r.value
      : { competitor: COMPETITOR_NAMES[i], price: null, url: null, in_stock: false, error: r.reason?.message }
  );
}

module.exports = { scrapeProduct, scrapeWithConfig, COMPETITOR_NAMES };
