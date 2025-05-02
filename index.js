const express = require('express');
const { chromium } = require('playwright');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { createClient } = require('@sanity/client');
const dayjs = require('dayjs');
require('dotenv').config({ path: '.env' });

const app = express();
const port = process.env.PORT || 8080;

const sanity = createClient({
  projectId: process.env.SANITY_PROJECT_ID,
  dataset: process.env.SANITY_DATASET,
  apiVersion: '2024-05-02',
  token: process.env.SANITY_API_TOKEN,
  useCdn: false,
});

const s3Client = new S3Client({
  region: 'auto',
  endpoint: process.env.CF_R2_ENDPOINT,
  credentials: {
    accessKeyId: process.env.CF_R2_ACCESS_KEY,
    secretAccessKey: process.env.CF_R2_SECRET_ACCESS_KEY,
  },
});

async function fetchStoreCodes() {
  const stores = await sanity.fetch('*[_type=="store" && defined(storeCode) && storeCode != ""]{storeCode}');
  return stores.map(store => store.storeCode);
}

async function scrapeAndUpload(storeCode) {
  console.log(`✅ Scraping başladı: ${storeCode}`);

  let browser;
  try {
    browser = await chromium.launch({ headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'] });
    const page = await browser.newPage();
    const imageUrls = new Set();

    await page.route('**/*', (route) => {
      const request = route.request();
      if (request.resourceType() === 'image') {
        const imgUrl = request.url();
        if (imgUrl.includes('akimages.shoplocal.com') && imgUrl.includes('1200.0.90.0') && !imgUrl.includes('HB')) {
          imageUrls.add(imgUrl);
        }
      }
      route.continue();
    });

    await page.goto(`https://aldi.us/weekly-specials/our-weekly-ads/?storeref=${storeCode}`, { waitUntil: 'networkidle' });
    await page.waitForTimeout(5000);
    await browser.close();

    const endDate = dayjs().add(7, 'day').format('YYYY-MM-DD');

    await Promise.all(Array.from(imageUrls).map(async (url) => {
      const res = await fetch(url);
      const buffer = Buffer.from(await res.arrayBuffer());
      const fileName = url.split('/').pop();
      const key = `aldi/${storeCode}/${endDate}/${fileName}`;

      await s3Client.send(new PutObjectCommand({
        Bucket: process.env.CF_R2_BUCKET,
        Key: key,
        Body: buffer,
        ContentType: 'image/jpeg',
      }));

      console.log(`🟢 Yüklendi: ${key}`);
    }));

    console.log(`🎉 İşlem tamamlandı: ${storeCode}`);

  } catch (error) {
    console.error(`❌ Hata oluştu: ${storeCode}`, error);
    if (browser) await browser.close();
  }
}

async function runBatchScraping(batch, batchNumber) {
  console.log(`🚀 Batch başladı: #${batchNumber}`);
  for (const storeCode of batch) {
    await scrapeAndUpload(storeCode);
  }
  console.log(`🏁 Batch tamamlandı: #${batchNumber}`);
}

app.get('/trigger-scrape', async (req, res) => {
  try {
    const storeCodes = await fetchStoreCodes();
    const BATCH_SIZE = 10;

    for (let i = 0, batchNumber = 1; i < storeCodes.length; i += BATCH_SIZE, batchNumber++) {
      const batch = storeCodes.slice(i, i + BATCH_SIZE);
      await runBatchScraping(batch, batchNumber);

      if ((batchNumber % 10) === 0) {
        console.log("🔄 100 store scrape edildi, sonraki batch için yeniden başlat.");
        break; // Railway cron otomatik yeniden başlatsın diye döngüyü durdur
      }
    }

    res.json({ message: 'Scraping başarıyla tamamlandı veya batch limiti doldu.' });

  } catch (error) {
    console.error('Genel hata:', error);
    res.status(500).json({ error: error.message });
  }
});

app.listen(port, '0.0.0.0', () => {
  console.log(`🌐 Server ${port} portunda çalışıyor.`);
});
