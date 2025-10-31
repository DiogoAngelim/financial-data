import processExchangeSafely from './importer.mjs'
import pLimit from 'p-limit';

const limit = pLimit(2);
const EXCHANGES = ['AU'];


async function main(exchanges) {
  for (const exchange of exchanges) {
    console.log(`\n📡 Processing ${exchange} safely...`);
    await limit(() => processExchangeSafely(exchange));
  }

  console.log('\n✅ All stock data fetched safely.');
}

main(EXCHANGES);
