const express = require('express');
const { chromium } = require('playwright');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { createClient } = require('@sanity/client');
const dayjs = require('dayjs');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 8080;

// Sanity client setup
const sanity = createClient({
  projectId: process.env.SANITY_PROJECT_ID,
  dataset: process.env.SANITY_DATASET,
  token: process.env.SANITY_API_TOKEN,
  useCdn: false,
  apiVersion: '2024-04-30'
});

// Cloudflare R2 setup
const s3Client = new S3Client({
  region: 'auto',
  endpoint: process.env.CF_R2_ENDPOINT,
  credentials: {
    accessKeyId: process.env.CF_R2_ACCESS_KEY,
    secretAccessKey: process.env.CF_R2_SECRET_ACCESS_KEY,
  },
});

// Sanity'den mağaza kodlarını al
async function fetchStoreCodes() {
  const stores = await sanity.fetch('*[_type=="store" && defined(storeCode)]{storeCode}');
  return stores.map(store => store.storeCode);
}

// Flyerları scrape et ve yükle
async function scrapeAndUpload(storeCode) {
  console.log(`🌐 Navigating to Aldi site for store code: ${storeCode}`);

  const browser = await chromium.launch();
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
  await page.waitForTimeout(8000);
  await browser.close();

  const endDate = dayjs().add(7, 'day').format('YYYY-MM-DD');
  console.log(`📸 Scraped ${imageUrls.size} images for store ${storeCode}.`);

  const uploadPromises = Array.from(imageUrls).map(async url => {
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

    console.log(`✅ Uploaded image: ${key}`);

    return `${process.env.CF_R2_PUBLIC_URL}/${key}`;
  });

  return Promise.all(uploadPromises);
}

// Tüm mağazalar için scraping işlemini başlat
async function runDailyJob() {
  const storeCodes = await fetchStoreCodes();
  console.log(`🔍 Found ${storeCodes.length} store codes.`);

  for (const code of storeCodes) {
    try {
      await scrapeAndUpload(code);
      console.log(`🎉 Successfully processed store: ${code}`);
    } catch (error) {
      console.error(`🚨 Error processing store ${code}:`, error);
    }
  }

  console.log('🚀 Daily scraping job completed successfully!');
}

// Cron ve manuel tetikleme için route
app.get('/trigger-scrape', (req, res) => {
  res.json({ message: '🕒 Scraping işlemi arka planda başlatıldı.' });
  runDailyJob().catch(console.error);
});

// Sağlık kontrolü (opsiyonel ama faydalıdır)
app.get('/health', (req, res) => {
  res.json({ status: '🟢 Healthy!' });
});

// Server başlatma
app.listen(port, '0.0.0.0', () => {
  console.log(`🚀 Server running on port ${port}`);
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
