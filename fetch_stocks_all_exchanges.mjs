import fs from 'fs';
import path from 'path';
import yahooFinance from 'yahoo-finance2';
import pLimit from 'p-limit';

const EXCHANGES = [
  'BR', 'AU', 'CA', 'CH', 'crypto', 'DE', 'forex', 'HK',
  'IN', 'JP', 'KSA', 'UK', 'US'
];

const BASE_DIR = path.join('optimalstocks', 'data');
if (!fs.existsSync(BASE_DIR)) fs.mkdirSync(BASE_DIR, { recursive: true });

const LOG_FILE = path.join(BASE_DIR, 'fetch_log.txt');
fs.writeFileSync(LOG_FILE, `Stock fetch log - ${new Date().toISOString()}\n\n`, 'utf8');

// Limit total concurrent requests to Yahoo Finance
const limit = pLimit(5);

async function fetchData(symbol, outputDir, retries = 3) {
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
      console.log(`No data for ${symbol}, skipping.`);
      fs.appendFileSync(LOG_FILE, `${symbol}: NO DATA\n`);
      return;
    }

    const rows = result.quotes.map(q => {
      const date = new Date(q.date).toISOString().split('T')[0];
      return [date, q.open, q.high, q.low, q.close, q.adjclose, q.volume].join(',');
    });

    const csvData = `Date,Open,High,Low,Close,Adj Close,Volume\n${rows.join('\n')}`;
    fs.writeFileSync(path.join(outputDir, `${symbol}.csv`), csvData, 'utf8');
    console.log(`✅ ${symbol} saved`);
    fs.appendFileSync(LOG_FILE, `${symbol}: SUCCESS\n`);

  } catch (error) {
    if (retries > 0) {
      console.log(`⚠️ Retry ${symbol} (${retries} left)`);
      await new Promise(r => setTimeout(r, 2000));
      return fetchData(symbol, outputDir, retries - 1);
    } else {
      console.error(`❌ Failed ${symbol}: ${error.message}`);
      fs.appendFileSync(LOG_FILE, `${symbol}: FAILED - ${error.message}\n`);
    }
  }
}

async function processExchange(exchange) {
  const fileName = `stocks_list_${exchange}.json`;
  if (!fs.existsSync(fileName)) {
    console.warn(`File not found: ${fileName}, skipping ${exchange}`);
    fs.appendFileSync(LOG_FILE, `${exchange}: FILE NOT FOUND\n`);
    return [];
  }

  const assets = JSON.parse(fs.readFileSync(fileName, 'utf8'));
  const outputDir = path.join(BASE_DIR, exchange);
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  return assets.map(asset => limit(() => fetchData(asset.symbol, outputDir)));
}

async function main() {
  let allPromises = [];
  for (const exchange of EXCHANGES) {
    console.log(`\n📡 Scheduling fetch for ${exchange}...`);
    const promises = await processExchange(exchange);
    allPromises = allPromises.concat(promises);
  }

  await Promise.all(allPromises);
  console.log('\n✅ All stock data fetched.');
  fs.appendFileSync(LOG_FILE, `\nAll fetches completed: ${new Date().toISOString()}\n`);
}

main();
