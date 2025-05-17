const express = require('express');
const { chromium } = require('playwright');
const { S3Client, PutObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const fetch = require('node-fetch');
const readline = require('readline');
const { Readable } = require('stream');
require('dotenv').config({ path: '.env' });

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

async function deletePreviousImages(folderPrefix) {
  const listParams = {
    Bucket: process.env.CF_R2_BUCKET,
    Prefix: `aldi/${folderPrefix}/`,
  };

  const { Contents } = await s3Client.send(new ListObjectsV2Command(listParams));
  if (Contents && Contents.length > 0) {
    for (const obj of Contents) {
      await s3Client.send(new DeleteObjectCommand({
        Bucket: process.env.CF_R2_BUCKET,
        Key: obj.Key,
      }));
      console.log(`🗑️ Silindi: ${obj.Key}`);
    }
  }
}

async function scrapeAndUploadFromUrl(flyerUrl) {
  const folderName = extractFilenameFromUrl(flyerUrl);
  const fullPrefix = `aldi/${folderName}`;

  console.log(`\n📍 ${flyerUrl}`);
  console.log(`🧹 ${fullPrefix} içeriği siliniyor...`);
  await deletePreviousImages(folderName);

  let browser;
  try {
    browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
    const page = await browser.newPage();
    const imageUrls = new Set();

    await page.route('**/*', (route) => {
      const req = route.request();
      if (req.resourceType() === 'image') {
        const imgUrl = req.url();
        if (imgUrl.includes('akimages.shoplocal.com') && imgUrl.includes('1200.0.90.0') && !imgUrl.includes('HB')) {
          imageUrls.add(imgUrl);
        }
      }
      route.continue();
    });

    await page.goto(flyerUrl, { waitUntil: 'networkidle' });
    await page.waitForTimeout(5000);
    await browser.close();

    for (const url of imageUrls) {
      const imgRes = await fetch(url);
      const buffer = Buffer.from(await imgRes.arrayBuffer());
      const fileName = url.split('/').pop();

      const key = `${fullPrefix}/${fileName}`;
      await s3Client.send(new PutObjectCommand({
        Bucket: process.env.CF_R2_BUCKET,
        Key: key,
        Body: buffer,
        ContentType: 'image/jpeg',
      }));

      console.log(`🟢 Yüklendi: ${key}`);
    }
  } catch (err) {
    console.error(`❌ Hata: ${flyerUrl}`, err);
    if (browser) await browser.close();
  }
}

app.get('/trigger-scrape', async (req, res) => {
  const links = await fetchLinks();

  for (const link of links) {
    await scrapeAndUploadFromUrl(link);
  }

  res.json({ status: 'Tüm linkler işlendi.' });
});

app.listen(port, () => {
  console.log(`🚀 Server ${port} portunda çalışıyor`);
});
