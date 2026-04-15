#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

function argValue(name, defaultValue = '') {
  const index = process.argv.indexOf(name);
  if (index === -1 || index + 1 >= process.argv.length) {
    return defaultValue;
  }
  return process.argv[index + 1];
}

function cleanText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function parseNetscapeCookies(filePath) {
  if (!filePath || !fs.existsSync(filePath)) {
    return [];
  }
  const lines = fs.readFileSync(filePath, 'utf8').split(/\r?\n/).filter(Boolean);
  const cookies = [];
  for (const line of lines) {
    if (line.startsWith('#') || !line.includes('\t')) continue;
    const parts = line.split('\t');
    if (parts.length < 7) continue;
    const [domain, _includeSubdomains, cookiePath, secure, expiration, name, value] = parts;
    cookies.push({
      domain,
      path: cookiePath || '/',
      name,
      value,
      expires: Number(expiration) || -1,
      secure: String(secure).toUpperCase() === 'TRUE',
      httpOnly: false,
      sameSite: 'Lax',
    });
  }
  return cookies;
}

function parseVideoId(value) {
  const text = String(value || '');
  const direct = text.match(/\/video\/(\d+)/);
  if (direct) return direct[1];
  const modal = text.match(/[?&]modal_id=(\d+)/);
  if (modal) return modal[1];
  const aweme = text.match(/[?&]aweme_id=(\d+)/);
  if (aweme) return aweme[1];
  return '';
}

function canonicalVideoUrl(videoId, fallbackUrl = '') {
  return videoId ? `https://www.douyin.com/video/${videoId}` : fallbackUrl;
}

function parseCreatorId(url) {
  const match = String(url || '').match(/\/user\/([^/?#]+)/);
  return match ? match[1] : '';
}

function epochToDate(epochSeconds) {
  const value = Number(epochSeconds || 0);
  if (!Number.isFinite(value) || value <= 0) return '';
  return new Date(value * 1000).toISOString().slice(0, 10);
}

function normalizeDesc(value) {
  return cleanText(String(value || '').replace(/#\S+/g, ' '));
}

function buildNetworkRecord(aweme, fallbackCreator) {
  const videoId = cleanText(aweme && aweme.aweme_id);
  if (!videoId) return null;
  const desc = cleanText(aweme && aweme.desc);
  const author = aweme && aweme.author ? aweme.author : {};
  return {
    creator_id: cleanText(author.sec_uid) || cleanText(fallbackCreator.creator_id),
    creator_name: cleanText(author.nickname) || cleanText(fallbackCreator.creator_name),
    creator_home_url: cleanText(fallbackCreator.creator_home_url),
    video_id: videoId,
    video_url: canonicalVideoUrl(videoId),
    title_detected: desc,
    publish_date: epochToDate(aweme && aweme.create_time),
    cover_text: normalizeDesc(desc),
    source_rank: 'network_aweme_post',
    inventory_status: 'discovered',
    inventory_notes: '',
  };
}

function buildContinuationUrl(lastSuccessfulUrl, nextCursor) {
  const nextUrl = new URL(lastSuccessfulUrl);
  nextUrl.searchParams.set('max_cursor', String(nextCursor || 0));
  nextUrl.searchParams.set('need_time_list', '0');
  nextUrl.searchParams.set('time_list_query', '0');
  return nextUrl.toString();
}

async function extractCreatorLink(page) {
  const anchors = await page.locator('a').evaluateAll((nodes) =>
    nodes
      .map((node) => ({ href: node.href || '', text: (node.innerText || '').trim() }))
      .filter((item) => item.href && item.href.includes('/user/MS4'))
  );
  return anchors.length ? anchors[0].href : '';
}

async function collectVisibleVideoAnchors(page) {
  const anchors = await page.locator('a').evaluateAll((nodes) =>
    nodes
      .map((node) => ({ href: node.href || '', text: (node.innerText || '').trim() }))
      .filter((item) => item.href)
  );
  const deduped = new Map();
  for (const anchor of anchors) {
    const videoId = parseVideoId(anchor.href);
    if (!videoId) continue;
    const videoUrl = canonicalVideoUrl(videoId, anchor.href);
    if (!deduped.has(videoId)) {
      deduped.set(videoId, {
        video_id: videoId,
        video_url: videoUrl,
        title_detected: cleanText(anchor.text),
        cover_text: cleanText(anchor.text),
        publish_date: '',
        source_rank: 'visible_anchor',
      });
    }
  }
  return Array.from(deduped.values());
}

async function collectEmbeddedVideoUrls(page) {
  const html = await page.content();
  const matches = html.match(/https:\/\/www\.douyin\.com\/video\/\d+/g) || [];
  return Array.from(new Set(matches)).map((videoUrl) => {
    const videoId = parseVideoId(videoUrl);
    return {
      video_id: videoId,
      video_url: videoUrl,
      title_detected: '',
      cover_text: '',
      publish_date: '',
      source_rank: 'html_fallback',
    };
  });
}

async function run() {
  const url = argValue('--url');
  const outputJson = argValue('--output-json');
  const scrollRounds = Number(argValue('--scroll-rounds', '12'));
  const waitMs = Number(argValue('--wait-ms', '1800'));
  const cookiesFile = argValue('--cookies-file');
  if (!url) {
    throw new Error('Missing --url');
  }

  const browser = await chromium.launch({ channel: 'chrome', headless: true });
  const context = await browser.newContext({
    viewport: { width: 1440, height: 2200 },
    locale: 'zh-CN',
  });
  const cookies = parseNetscapeCookies(cookiesFile).filter((cookie) => cookie.domain.includes('douyin.com'));
  if (cookies.length) {
    await context.addCookies(cookies);
  }
  const page = await context.newPage();
  const inventoryMap = new Map();
  const networkPageCursorSeen = new Set();
  const responseTasks = [];
  let networkPageCount = 0;
  let lastSuccessfulPageUrl = '';
  let lastPayloadHasMore = 0;
  let lastPayloadMaxCursor = 0;

  page.on('response', (response) => {
    responseTasks.push((async () => {
      const responseUrl = response.url();
      if (!responseUrl.includes('/aweme/v1/web/aweme/post/')) {
        return;
      }
      try {
        const rawText = await response.text();
        const normalizedText = cleanText(rawText);
        if (!normalizedText || normalizedText === '{"status_code":0}') {
          return;
        }
        const payload = JSON.parse(rawText);
        const pageKey = `${payload.max_cursor || 0}:${payload.min_cursor || 0}:${payload.has_more || 0}`;
        if (!networkPageCursorSeen.has(pageKey)) {
          networkPageCursorSeen.add(pageKey);
          networkPageCount += 1;
        }
        lastSuccessfulPageUrl = responseUrl;
        lastPayloadHasMore = Number(payload.has_more || 0);
        lastPayloadMaxCursor = Number(payload.max_cursor || 0);
        const awemeList = Array.isArray(payload.aweme_list) ? payload.aweme_list : [];
        for (const aweme of awemeList) {
          const record = buildNetworkRecord(aweme, {
            creator_id: creatorId,
            creator_name: creatorName,
            creator_home_url: creatorHomeUrl || page.url(),
          });
          if (!record) continue;
          inventoryMap.set(record.video_id || record.video_url, record);
        }
      } catch (error) {
        notes.push(`failed to parse aweme/post response: ${cleanText(error && error.message ? error.message : String(error))}`);
      }
    })());
  });

  const notes = [];
  let creatorHomeUrl = '';
  let creatorId = '';
  let creatorName = '';

  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
  await page.waitForTimeout(5000);

  const inferredCreatorLink = await extractCreatorLink(page);
  if (inferredCreatorLink && inferredCreatorLink !== url) {
    creatorHomeUrl = inferredCreatorLink;
    await page.goto(`${creatorHomeUrl}?from_tab_name=main&showTab=record`, {
      waitUntil: 'domcontentloaded',
      timeout: 45000,
    });
    await page.waitForTimeout(5000);
  } else {
    creatorHomeUrl = page.url();
  }

  creatorId = parseCreatorId(creatorHomeUrl || page.url());
  const bodyInitial = await page.locator('body').innerText().catch(() => '');
  const initialText = cleanText(bodyInitial);
  const nameMatch = bodyInitial.match(/\n([^\n]{1,30})\n关注\n(?:\d+|粉丝)/);
  creatorName = nameMatch ? cleanText(nameMatch[1]) : cleanText((await page.title()).replace(/的抖音\s*-\s*抖音$/, ''));

  if (initialText.includes('服务异常')) {
    notes.push('creator page shows service exception in works area');
  }
  if (initialText.includes('验证码中间页')) {
    notes.push('creator page blocked by captcha');
  }
  for (let round = 0; round < scrollRounds; round += 1) {
    await page.mouse.wheel(0, 1800);
    await page.waitForTimeout(waitMs);
  }
  if (responseTasks.length) {
    await Promise.allSettled(responseTasks);
  }

  let continuationRounds = 0;
  let noGrowthRounds = 0;
  const continuationLimit = 18;
  while (
    lastSuccessfulPageUrl &&
    lastPayloadHasMore &&
    continuationRounds < continuationLimit &&
    noGrowthRounds < 3
  ) {
    const beforeCount = inventoryMap.size;
    const continuationUrl = buildContinuationUrl(lastSuccessfulPageUrl, lastPayloadMaxCursor);
    let payload = null;
    try {
      const rawText = await page.evaluate(async (requestUrl) => {
        const response = await fetch(requestUrl, { credentials: 'include' });
        return await response.text();
      }, continuationUrl);
      const normalizedText = cleanText(rawText);
      if (!normalizedText || normalizedText === '{"status_code":0}') {
        noGrowthRounds += 1;
        continuationRounds += 1;
        continue;
      }
      payload = JSON.parse(rawText);
    } catch (error) {
      notes.push(`continuation fetch failed: ${cleanText(error && error.message ? error.message : String(error))}`);
      break;
    }

    const awemeList = Array.isArray(payload.aweme_list) ? payload.aweme_list : [];
    for (const aweme of awemeList) {
      const record = buildNetworkRecord(aweme, {
        creator_id: creatorId,
        creator_name: creatorName,
        creator_home_url: creatorHomeUrl || page.url(),
      });
      if (!record) continue;
      inventoryMap.set(record.video_id || record.video_url, record);
    }

    continuationRounds += 1;
    if (inventoryMap.size === beforeCount) {
      noGrowthRounds += 1;
    } else {
      noGrowthRounds = 0;
    }
    lastSuccessfulPageUrl = continuationUrl;
    lastPayloadHasMore = Number(payload.has_more || 0);
    lastPayloadMaxCursor = Number(payload.max_cursor || 0);
  }

  if (!inventoryMap.size) {
    notes.push('no creator video records recovered from aweme/post network responses');
  }

  const bodyFinal = await page.locator('body').innerText().catch(() => '');
  if (!inventoryMap.size) {
    if (cleanText(bodyFinal).includes('服务异常')) {
      notes.push('no visible video cards discovered because works area stayed blocked');
    } else {
      notes.push('no visible video cards discovered from creator page');
    }
  }
  if (networkPageCount) {
    notes.push(`captured ${networkPageCount} aweme/post response page(s)`);
  }
  if (continuationRounds) {
    notes.push(`requested ${continuationRounds} continuation page(s) after initial scroll capture`);
  }
  if (!lastPayloadHasMore && inventoryMap.size) {
    notes.push('stopped because has_more=false');
  } else if (noGrowthRounds >= 3) {
    notes.push('stopped because 3 continuation rounds produced no new video ids');
  } else if (continuationRounds >= continuationLimit) {
    notes.push(`stopped because continuation limit ${continuationLimit} was reached`);
  }

  const records = Array.from(inventoryMap.values())
    .map((item) => ({
      creator_id: cleanText(item.creator_id || creatorId),
      creator_name: cleanText(item.creator_name || creatorName),
      creator_home_url: cleanText(item.creator_home_url || creatorHomeUrl || page.url()),
      video_id: cleanText(item.video_id),
      video_url: cleanText(item.video_url),
      title_detected: cleanText(item.title_detected),
      publish_date: cleanText(item.publish_date),
      cover_text: cleanText(item.cover_text),
      source_rank: cleanText(item.source_rank),
      inventory_status: cleanText(item.inventory_status || (item.video_url ? 'discovered' : 'partial')),
      inventory_notes: cleanText(item.inventory_notes),
    }))
    .sort((a, b) => String(a.video_id || a.video_url).localeCompare(String(b.video_id || b.video_url)));

  const result = {
    input_url: url,
    final_url: page.url(),
    creator_home_url: creatorHomeUrl || page.url(),
    creator_id: creatorId,
    creator_name: creatorName,
    discovered_count: records.length,
    deduped_count: records.length,
    notes,
    records,
  };

  if (outputJson) {
    fs.writeFileSync(outputJson, JSON.stringify(result, null, 2), 'utf8');
  }
  process.stdout.write(JSON.stringify(result, null, 2));

  await context.close();
  await browser.close();
}

run().catch((error) => {
  const payload = {
    input_url: argValue('--url'),
    final_url: '',
    creator_home_url: '',
    creator_id: '',
    creator_name: '',
    discovered_count: 0,
    deduped_count: 0,
    notes: [cleanText(error && error.message ? error.message : String(error))],
    records: [],
  };
  process.stdout.write(JSON.stringify(payload, null, 2));
  process.exit(1);
});
