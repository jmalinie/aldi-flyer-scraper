const { createClient } = require('@sanity/client');
require('dotenv').config();

const sanity = createClient({
  projectId: process.env.SANITY_PROJECT_ID,
  dataset: process.env.SANITY_DATASET,
  apiVersion: '2024-04-30',
  token: process.env.SANITY_API_TOKEN,
  useCdn: false,
});

async function checkStoreCodes() {
  const stores = await sanity.fetch('*[_type=="store" && defined(storeCode)]{storeCode}');
  console.log(`Toplam store kodu sayısı: ${stores.length}`);
}

checkStoreCodes().catch(console.error);
