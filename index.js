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
  apiVersion: '2024-04-30',
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
  return stores
    .map(store => store.storeCode)
    .filter(code => code && code.trim().length > 0); // boş kodları çıkar
}

async function scrapeAndUpload(storeCode) {
  console.log(`✅ Scraping başladı: Store kodu: ${storeCode}`);

  let browser;
  try {
    browser = await chromium.launch({
      headless: true,
      args: ['--disable-dev-shm-usage', '--no-sandbox'],
    });
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

    console.log(`🌐 Aldi sitesine gidiliyor: ${storeCode}`);
    await page.goto(`https://aldi.us/weekly-specials/our-weekly-ads/?storeref=${storeCode}`, { waitUntil: 'networkidle' });
    await page.waitForTimeout(8000); // bekleme süresi optimize edildi
    console.log(`⏳ Aldi sitesinden çıkıldı: ${storeCode}`);
    await browser.close();

    const endDate = dayjs().add(7, 'day').format('YYYY-MM-DD');
    console.log(`📦 Upload işlemi başlıyor: ${storeCode}, toplam ${imageUrls.size} resim bulundu.`);

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

    await Promise.all(uploadPromises);
    console.log(`🎉 İşlem tamamlandı: ${storeCode}`);

  } catch (error) {
    console.error(`❌ Hata oluştu, Store kodu: ${storeCode}`, error);
    if (browser) await browser.close();
  }
}

async function runDailyJob() {
  const storeCodes = await fetchStoreCodes();
  const BATCH_SIZE = 20;
  const CONCURRENT_LIMIT = 5;

  for (let i = 0; i < storeCodes.length; i += BATCH_SIZE) {
    const batch = storeCodes.slice(i, i + BATCH_SIZE);

    for (let j = 0; j < batch.length; j += CONCURRENT_LIMIT) {
      const concurrentBatch = batch.slice(j, j + CONCURRENT_LIMIT);

      await Promise.all(concurrentBatch.map(code =>
        scrapeAndUpload(code)
          .then(() => console.log(`🟢 Başarılı: ${code}`))
          .catch(e => console.error(`🔴 Hata: ${code}`, e))
      ));
    }

    console.log(`✅ Batch tamamlandı: ${i + 1}-${Math.min(i + BATCH_SIZE, storeCodes.length)}`);
  }

  await writeLogToR2(`🎉 Daily scraping tamamlandı: ${new Date().toISOString()}`);
}

async function writeLogToR2(logMessage) {
  const key = `logs/${new Date().toISOString()}.txt`;
  await s3Client.send(new PutObjectCommand({
    Bucket: process.env.CF_R2_BUCKET,
    Key: key,
    Body: logMessage,
    ContentType: 'text/plain',
  }));
}

app.get('/trigger-scrape', (req, res) => {
  runDailyJob().catch(console.error);
  res.json({ message: 'Scraping başlatıldı.' });
});

app.listen(port, '0.0.0.0', () => {
  console.log(`🌐 Server ${port} portunda çalışıyor.`);
});
