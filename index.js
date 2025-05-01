const express = require('express');
const { chromium } = require('playwright');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { createClient } = require('@sanity/client');
const dayjs = require('dayjs');
require('dotenv').config({ path: '.env' });

console.log("Sanity Project ID:", process.env.SANITY_PROJECT_ID);
console.log("Sanity Dataset:", process.env.SANITY_DATASET);

const app = express();
const port = process.env.PORT || 8080;

const sanity = createClient({
  projectId: process.env.SANITY_PROJECT_ID,
  dataset: process.env.SANITY_DATASET,
  apiVersion: '2024-04-30', // Sanity için gerekli API versiyonu belirtildi
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
  const stores = await sanity.fetch('*[_type=="store" && defined(storeCode)]{storeCode}');
  return stores.map(store => store.storeCode);
}

async function scrapeAndUpload(storeCode) {
  console.log(`✅ Scraping başladı: Store kodu: ${storeCode}`);

  const browser = await chromium.launch({ headless: true });
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
  await page.waitForTimeout(10000); // 10 saniye bekleme
  await browser.close();

  const endDate = dayjs().add(7, 'day').format('YYYY-MM-DD');

  const uploadPromises = Array.from(imageUrls).map(async (url) => {
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

    return `${process.env.CF_R2_PUBLIC_URL}/${key}`;
  });

  return Promise.all(uploadPromises);
}

async function runDailyJob() {
  const storeCodes = await fetchStoreCodes();
  for (const code of storeCodes) {
    try {
      await scrapeAndUpload(code);
    } catch (error) {
      console.error(`❌ Store kodu hata: ${code}`, error);
    }
  }
  console.log('🎉 Daily scraping tamamlandı.');
}

// HTTP Endpoint'i (tetikleme için)
app.get('/trigger-scrape', (req, res) => {
  runDailyJob().catch(console.error);
  res.json({ message: 'Scraping başlatıldı.' });
});

// Ana giriş noktası (Railway cron için idealdir)
runDailyJob().catch(console.error);

app.listen(port, '0.0.0.0', () => {
  console.log(`🌐 Server ${port} portunda çalışıyor.`);
});

async function writeLogToR2(logMessage) {
  const key = `logs/${new Date().toISOString()}.txt`;
  await s3Client.send(new PutObjectCommand({
    Bucket: process.env.CF_R2_BUCKET,
    Key: key,
    Body: logMessage,
    ContentType: 'text/plain',
  }));
}
