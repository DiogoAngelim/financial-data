import fs from 'fs';
import path from 'path';
import yahooFinance from 'yahoo-finance2';
import pLimit from 'p-limit';

const EXCHANGES = ['crypto'];

const BASE_DIR = path.join('.', 'public');
if (!fs.existsSync(BASE_DIR)) fs.mkdirSync(BASE_DIR, { recursive: true });

const limit = pLimit(2); // max 2 concurrent fetches

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Updated fetchData with rate-limit handling
async function fetchData(symbol, outputDir) {
  const today = new Date();
  const twoYearsAgo = new Date();
  twoYearsAgo.setFullYear(today.getFullYear() - 2);

  try {
    const result = await yahooFinance.chart(symbol, {
      period1: twoYearsAgo,
      period2: today,
      interval: '1d',
    });

    if (!result?.quotes?.length) {
      console.warn(`⚠️ No data for ${symbol}, skipping.`);
      return { symbol, status: 'noData' };
    }

    const rows = result.quotes.map(q => {
      const date = new Date(q.date).toISOString().split('T')[0];
      return [date, q.open, q.high, q.low, q.close, q.adjclose, q.volume].join(',');
    });

    const csvData = `Date,Open,High,Low,Close,Adj Close,Volume\n${rows.join('\n')}`;
    fs.writeFileSync(path.join(outputDir, `${symbol}.csv`), csvData, 'utf8');
    console.log(`✅ ${symbol} saved`);
    return { symbol, status: 'success' };

  } catch (error) {
    const msg = error?.message || '';
    const statusCode = error?.response?.status;

    if (msg.includes('No data found') || statusCode === 404) {
      console.warn(`❌ ${symbol} not found.`);
      return { symbol, status: '404' };
    }

    if (msg.includes('Too Many Requests') || statusCode === 429) {
      console.warn(`⚠️ ${symbol} hit rate limit.`);
      return { symbol, status: 'rateLimited' };
    }

    console.error(`⚠️ Error fetching ${symbol}: ${msg}`);
    return { symbol, status: 'error' };
  }
}

// Process symbols in a safe, sequential manner with retries
async function processExchangeSafely(exchange) {
  const fileName = path.join(BASE_DIR, `stocks_list_${exchange}.json`);
  if (!fs.existsSync(fileName)) {
    console.warn(`File not found: ${fileName}, skipping ${exchange}`);
    return [];
  }

  let assets = JSON.parse(fs.readFileSync(fileName, 'utf8'));
  const outputDir = path.join(BASE_DIR, exchange);
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  const results = [];

  for (const asset of assets) {
    const symbol = asset.symbol;
    let status;

    for (let attempt = 0; attempt < 3; attempt++) {
      const result = await fetchData(symbol, outputDir);

      if (result.status === 'success') {
        status = 'success';
        break;
      }

      if (result.status === 'rateLimited') {
        const waitTime = 5000 + Math.random() * 5000; // 5–10s wait
        console.warn(`⚠️ ${symbol} hit rate limit. Waiting ${Math.round(waitTime / 1000)}s before retry...`);
        await delay(waitTime);
        continue;
      }

      if (result.status === 'error') {
        const waitTime = 2000 + Math.random() * 2000; // 2–4s retry delay
        console.log(`⚠️ Retry ${symbol} (${2 - attempt} left) after ${Math.round(waitTime / 1000)}s...`);
        await delay(waitTime);
      }
    }

    if (!status) {
      console.error(`❌ Failed ${symbol} after retries.`);
      status = 'failed';
    }

    results.push({ symbol, status });

    // Small randomized delay between symbols
    await delay(500 + Math.random() * 1500); // 0.5–2s
  }

  // Remove assets that returned 404
  const removed = results.filter(r => r.status === '404').map(r => r.symbol);
  if (removed.length) {
    assets = assets.filter(a => !removed.includes(a.symbol));
    fs.writeFileSync(fileName, JSON.stringify(assets, null, 2), 'utf8');
    console.log(`Removed ${removed.length} assets from ${fileName}`);
  }

  return results;
}

// Main pipeline with concurrency control across exchanges
async function main() {
  for (const exchange of EXCHANGES) {
    console.log(`\n📡 Processing ${exchange} safely...`);
    await limit(() => processExchangeSafely(exchange));
  }

  console.log('\n✅ All stock data fetched safely.');
}

main();
