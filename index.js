import express from 'express';
import { chromium } from 'playwright';
import {
  S3Client,
  PutObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command
} from '@aws-sdk/client-s3';
import fetch from 'node-fetch';
import dotenv from 'dotenv';
import pLimit from 'p-limit';
dotenv.config({ path: '.env' });

const app = express();
const port = process.env.PORT || 8080;

const s3Client = new S3Client({
  region: 'auto',
  endpoint: process.env.CF_R2_ENDPOINT,
  credentials: {
    accessKeyId: process.env.CF_R2_ACCESS_KEY,
    secretAccessKey: process.env.CF_R2_SECRET_ACCESS_KEY,
  },
});

async function fetchLinks() {
  const res = await fetch(process.env.ALDI_LINKS_URL);
  const text = await res.text();
  return text.split('\n').map(x => x.trim()).filter(Boolean);
}

function extractFilenameFromUrl(url) {
  const urlObj = new URL(url);
  const lat = urlObj.searchParams.get('latitude');
  const lon = urlObj.searchParams.get('longitude');
  return `latitude=${lat}&longitude=${lon}`;
}

async function scrapeAndUploadFromUrl(flyerUrl) {
  const folderName = extractFilenameFromUrl(flyerUrl);
  const fullPrefix = `aldi/${folderName}`;
  console.log(`\n📍 ${flyerUrl}`);
  console.log(`📂 Klasör: ${fullPrefix}`);

  const existingList = await s3Client.send(new ListObjectsV2Command({
    Bucket: process.env.CF_R2_BUCKET,
    Prefix: `${fullPrefix}/`
  }));

  const existingFiles = new Map();
  for (const obj of existingList.Contents || []) {
    const name = obj.Key.split('/').pop();
    existingFiles.set(name, obj.Size);
  }

  let browser;
  try {
    browser = await chromium.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-dev-shm-usage',
        '--single-process',
        '--disable-gpu'
      ]
    });
    const page = await browser.newPage();
    const imageUrls = new Set();

    await page.route('**/*', (route) => {
      const req = route.request();
      const imgUrl = req.url();
      if (
        req.resourceType() === 'image' &&
        imgUrl.includes('akimages.shoplocal.com') &&
        imgUrl.includes('1200.0.90.0') &&
        !imgUrl.includes('HB')
      ) {
        imageUrls.add(imgUrl);
      }
      route.continue();
    });

    await Promise.race([
      page.goto(flyerUrl, { waitUntil: 'domcontentloaded' }),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Timeout')), 180000))
    ]);

    await page.waitForTimeout(10000);
  } catch (err) {
    console.error(`❌ Sayfa hatası: ${flyerUrl}`, err);
    throw err;
  } finally {
    if (browser) await browser.close();
  }

  const uploadedFiles = new Set();

  for (const url of imageUrls) {
    try {
      const fileName = url.split('/').pop();
      uploadedFiles.add(fileName);

      const imgRes = await fetch(url);
      const buffer = Buffer.from(await imgRes.arrayBuffer());

      if (existingFiles.has(fileName) && existingFiles.get(fileName) === buffer.length) {
        console.log(`⏭️ Atlandı (değişmedi): ${fileName}`);
        continue;
      }

      const key = `${fullPrefix}/${fileName}`;
      await s3Client.send(new PutObjectCommand({
        Bucket: process.env.CF_R2_BUCKET,
        Key: key,
        Body: buffer,
        ContentType: 'image/jpeg',
      }));

      console.log(`🟢 Yüklendi: ${key}`);
    } catch (uploadError) {
      console.error(`🚫 Yükleme hatası: ${url}`, uploadError);
    }
  }

  for (const [fileName] of existingFiles) {
    if (!uploadedFiles.has(fileName)) {
      const deleteKey = `${fullPrefix}/${fileName}`;
      await s3Client.send(new DeleteObjectCommand({
        Bucket: process.env.CF_R2_BUCKET,
        Key: deleteKey
      }));
      console.log(`🗑️ Silindi (artık yok): ${deleteKey}`);
    }
  }
}

async function scrapeWithRetry(url, maxAttempts = 3) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      console.log(`🔁 ${attempt}. deneme: ${url}`);
      await scrapeAndUploadFromUrl(url);
      await new Promise(r => setTimeout(r, 2000));
      return true;
    } catch (err) {
      console.log(`⛔ ${attempt}. deneme başarısız: ${url}`);
      if (attempt === maxAttempts) return false;
    }
  }
}

app.get('/trigger-scrape', async (req, res) => {
  const links = await fetchLinks();
  const failed = [];
  const retryQueue = [];
  const limit = pLimit(1);

  console.time('Tüm işlem süresi');

  const tasks = links.map((link, index) =>
    limit(async () => {
      const success = await scrapeWithRetry(link, 1);
      if (!success) retryQueue.push(link);

      if (index > 0 && index % 10 === 0) {
        console.log(`⏳ ${index}. link sonrası dinlenme`);
        await new Promise(r => setTimeout(r, 3000));
      }
    })
  );

  await Promise.allSettled(tasks);

  if (retryQueue.length > 0) {
    console.log(`🚨 İlk turdan kalan başarısız linkler yeniden deneniyor...`);
    const retryTasks = retryQueue.map((link, idx) =>
      limit(async () => {
        const retrySuccess = await scrapeWithRetry(link, 2);
        if (!retrySuccess) console.log(`❌ Yeniden de başarısız: ${link}`);
        if (idx > 0 && idx % 10 === 0) {
          console.log(`⏳ Retry içinde kısa dinlenme`);
          await new Promise(r => setTimeout(r, 3000));
        }
      })
    );
    await Promise.allSettled(retryTasks);
  }

  console.timeEnd('Tüm işlem süresi');
  res.json({ status: 'İşlem tamamlandı.' });
});

app.listen(port, () => {
  console.log(`🚀 Server ${port} portunda çalışıyor`);
});
