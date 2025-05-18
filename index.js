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

  // 1. R2 klasöründeki mevcut dosyaları al
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
    browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
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

    try {
      await page.goto(flyerUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
      await page.waitForTimeout(5000);
    } catch (gotoError) {
      console.error(`❌ GOTO hatası: ${flyerUrl}`, gotoError);
      await browser.close();
      throw gotoError;
    }

    await browser.close();

    const uploadedFiles = new Set();

    for (const url of imageUrls) {
      try {
        const fileName = url.split('/').pop();
        uploadedFiles.add(fileName);

        const imgRes = await fetch(url);
        const buffer = Buffer.from(await imgRes.arrayBuffer());

        // Aynı dosya zaten varsa ve boyutu da aynıysa tekrar yükleme
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

    // 3. Artık olmayan eski dosyaları sil
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

  } catch (err) {
    console.error(`❌ Genel hata: ${flyerUrl}`, err);
    if (browser) await browser.close();
    throw err;
  }
}

// Retry destekli sürüm
async function scrapeWithRetry(url, maxAttempts = 3) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      console.log(`🔁 ${attempt}. deneme: ${url}`);
      await scrapeAndUploadFromUrl(url);
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

  for (const link of links) {
    const success = await scrapeWithRetry(link);
    if (!success) failed.push(link);
  }

  if (failed.length > 0) {
    console.log(`🚨 İlk turda başarısız olan ${failed.length} link yeniden deneniyor...`);

    const stillFailed = [];
    for (const link of failed) {
      const retrySuccess = await scrapeWithRetry(link, 3);
      if (!retrySuccess) stillFailed.push(link);
    }

    console.log(`❌ Hâlâ başarısız olan ${stillFailed.length} link:`);
    stillFailed.forEach(l => console.log(`- ${l}`));
  }

  res.json({ status: 'İşlem tamamlandı.' });
});

app.listen(port, () => {
  console.log(`🚀 Server ${port} portunda çalışıyor`);
});
