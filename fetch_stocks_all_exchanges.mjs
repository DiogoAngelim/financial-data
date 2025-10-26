import fs from 'fs';
import path from 'path';
import yahooFinance from 'yahoo-finance2';
import pLimit from 'p-limit';

const EXCHANGES = [
  'BR', 'AU', 'CA', 'CH', 'crypto', 'DE', 'forex', 'HK',
  'IN', 'JP', 'KSA', 'UK', 'US'
];

const BASE_DIR = process.cwd();
const LOG_FILE = path.join(BASE_DIR, 'fetch.log');

const limit = pLimit(5);

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
      fs.appendFileSync(LOG_FILE, `${symbol}: NO DATA\n`);
      return { symbol, status: 'noData' };
    }

    const rows = result.quotes.map(q => {
      const date = new Date(q.date).toISOString().split('T')[0];
      return [date, q.open, q.high, q.low, q.close, q.adjclose, q.volume].join(',');
    });

    const csvData = `Date,Open,High,Low,Close,Adj Close,Volume\n${rows.join('\n')}`;
    fs.writeFileSync(path.join(outputDir, `${symbol}.csv`), csvData, 'utf8');
    console.log(`✅ ${symbol} saved`);
    fs.appendFileSync(LOG_FILE, `${symbol}: SUCCESS\n`);
    return { symbol, status: 'success' };

  } catch (error) {
    const msg = error?.message || '';
    if (msg.includes('No data found') || error?.response?.status === 404) {
      console.warn(`❌ ${symbol} not found.`);
      fs.appendFileSync(LOG_FILE, `${symbol}: NOT FOUND\n`);
      return { symbol, status: '404' };
    }

    console.error(`⚠️ Error fetching ${symbol}: ${msg}`);
    return { symbol, status: 'error' }; // don’t throw, let retries handle
  }
}

async function processExchange(exchange) {
  const fileName = `stocks_list_${exchange}.json`;
  if (!fs.existsSync(fileName)) {
    console.warn(`File not found: ${fileName}, skipping ${exchange}`);
    fs.appendFileSync(LOG_FILE, `${exchange}: FILE NOT FOUND\n`);
    return [];
  }

  let assets = JSON.parse(fs.readFileSync(fileName, 'utf8'));
  const outputDir = path.join(BASE_DIR, exchange);
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  const results = await Promise.all(
    assets.map(asset =>
      limit(async () => {
        let symbol = asset.symbol;
        let status;

        for (let attempt = 0; attempt < 3; attempt++) {
          const result = await fetchData(symbol, outputDir);

          if (result.status === 'success') {
            status = 'success';
            break;
          }

          // Retry crypto with -USD if first attempt failed
          if (
            exchange === 'crypto' &&
            attempt === 0 &&
            result.status === '404' &&
            !symbol.endsWith('-USD')
          ) {
            const newSymbol = `${symbol}-USD`;
            console.log(`🔁 Retrying ${symbol} as ${newSymbol}...`);
            const retryResult = await fetchData(newSymbol, outputDir);

            if (retryResult.status === 'success') {
              console.log(`🔄 Updating ${symbol} -> ${newSymbol} in ${fileName}`);
              fs.appendFileSync(LOG_FILE, `${symbol}: UPDATED TO ${newSymbol}\n`);

              assets = assets.filter(a => a.symbol !== symbol);
              assets.push({ ...asset, symbol: newSymbol });
              fs.writeFileSync(fileName, JSON.stringify(assets, null, 2), 'utf8');

              const oldPath = path.join(outputDir, `${symbol}.csv`);
              const newPath = path.join(outputDir, `${newSymbol}.csv`);
              if (fs.existsSync(oldPath)) fs.renameSync(oldPath, newPath);

              symbol = newSymbol;
              status = 'success';
              break;
            }
          }

          console.log(`⚠️ Retry ${symbol} (${2 - attempt} left)`);
          await new Promise(r => setTimeout(r, 2000));
        }

        if (!status) {
          console.error(`❌ Failed ${symbol} after retries.`);
          fs.appendFileSync(LOG_FILE, `${symbol}: FAILED AFTER RETRIES\n`);
          status = 'failed';
        }

        return { symbol, status };
      })
    )
  );

  // Remove all assets that returned 404
  const removed = results.filter(r => r.status === '404').map(r => r.symbol);
  if (removed.length) {
    assets = assets.filter(a => !removed.includes(a.symbol));
    fs.writeFileSync(fileName, JSON.stringify(assets, null, 2), 'utf8');
    console.log(`Removed ${removed.length} assets from ${fileName}`);
  }

  return results;
}

async function main() {
  for (const exchange of EXCHANGES) {
    console.log(`\n📡 Scheduling fetch for ${exchange}...`);
    await processExchange(exchange);
  }

  console.log('\n✅ All stock data fetched.');
  fs.appendFileSync(LOG_FILE, `\nAll fetches completed: ${new Date().toISOString()}\n`);
}

main();
