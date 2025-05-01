require('dotenv').config({ path: '.env' });

console.log("✅ Env dosya testi:");
console.log("Project ID:", process.env.SANITY_PROJECT_ID);
console.log("Dataset:", process.env.SANITY_DATASET);
console.log("R2 Bucket:", process.env.CF_R2_BUCKET);
