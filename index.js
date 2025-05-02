const express = require('express');
const { chromium } = require('playwright');
const { S3Client, PutObjectCommand, GetObjectCommand, PutObjectAclCommand } = require('@aws-sdk/client-s3');
const { createClient } = require('@sanity/client');
const dayjs = require('dayjs');
const streamToString = require('stream-to-string');
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

async function getCurrentIndex() {
  try {
    const response = await s3Client.send(new GetObjectCommand({
      Bucket: process.env.CF_R2_BUCKET,
      Key: 'state/currentIndex.txt',
    }));
    const indexStr = await streamToString(response.Body);
    return parseInt(indexStr, 10);
  } catch (error) {
    return 0; // İlk defa çalıştırılıyorsa 0 döndür
  }
}

async function saveCurrentIndex(index) {
  await s3Client.send(new PutObjectCommand({
    Bucket: process.env.CF_R2_BUCKET,
    Key: 'state/currentIndex.txt',
    Body: index.toString(),
    ContentType: 'text/plain',
  }));
}

async function runBatchScraping(batch, batchNumber) {
  console.log(`🚀 Batch başladı: #${batchNumber}`);
  for (const storeCode of batch) {
    await scrapeAndUpload(storeCode);
  }
  console.log(`🏁 Batch tamamlandı: #${batchNumber}`);
}

async function runDailyJob() {
  try {
    const storeCodes = await fetchStoreCodes();
    const BATCH_SIZE = 10;
    const MAX_STORES_PER_RUN = 100;

    let currentIndex = await getCurrentIndex();

    let processedCount = 0;
    while (currentIndex < storeCodes.length && processedCount < MAX_STORES_PER_RUN) {
      const batch = storeCodes.slice(currentIndex, currentIndex + BATCH_SIZE);
      await runBatchScraping(batch, (currentIndex / BATCH_SIZE) + 1);

      currentIndex += BATCH_SIZE;
      processedCount += BATCH_SIZE;

      await saveCurrentIndex(currentIndex);
    }

    if (currentIndex >= storeCodes.length) {
      await saveCurrentIndex(0); // Tüm storelar tamamlandı, tekrar başa dön
    }

    console.log('🎉 Cron job scraping işlemi tamamlandı.');
    process.exit(0);

  } catch (error) {
    console.error('🔴 Cron job scraping işleminde hata:', error);
    process.exit(1);
  }
}

app.get('/trigger-scrape', async (req, res) => {
  await runDailyJob();
  res.json({ message: 'Scraping başarıyla tamamlandı veya batch limiti doldu.' });
});

app.listen(port, '0.0.0.0', () => {
  console.log(`🌐 Server ${port} portunda çalışıyor.`);
});

if (require.main === module) {
  runDailyJob();
}
