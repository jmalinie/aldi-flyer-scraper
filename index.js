const express = require('express');
const { chromium } = require('playwright');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const dayjs = require('dayjs');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

const s3Client = new S3Client({
  region: 'auto',
  endpoint: process.env.CF_R2_ENDPOINT,
  credentials: {
    accessKeyId: process.env.CF_R2_ACCESS_KEY,
    secretAccessKey: process.env.CF_R2_SECRET_ACCESS_KEY,
  },
});

// Flyer'ları Aldi sitesinden kazı ve R2'ye yükle
async function scrapeAndUpload(storeCode) {
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

  const endDate = dayjs().add(7, 'day').format('YYYY-MM-DD'); // örnek: flyerlar genelde 7 gün geçerli
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

    return `${process.env.CF_R2_PUBLIC_URL}/${key}`;
  });

  return Promise.all(uploadPromises);
}

app.get('/scrape-aldi/:storeCode', async (req, res) => {
  try {
    const urls = await scrapeAndUpload(req.params.storeCode);
    res.json({ message: 'Flyers scraped & uploaded', urls });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message });
  }
});

app.listen(port, () => {
  console.log(`Server running on ${port}`);
});
