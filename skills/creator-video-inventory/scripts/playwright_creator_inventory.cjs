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

function hasFlag(name) {
  return process.argv.includes(name);
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

function buildNetworkRecord(aweme, fallbackCreator, sourceBucket = '') {
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
    source_rank: sourceBucket ? 'network_aweme_post_bucket' : 'network_aweme_post',
    inventory_status: 'discovered',
    inventory_notes: sourceBucket ? `source_bucket=${sourceBucket}` : '',
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

function buildCreatorPostUrl(creatorId, maxCursor = 0, count = 18) {
  return `https://www.douyin.com/aweme/v1/web/aweme/post/?device_platform=webapp&aid=6383&channel=channel_pc_web&sec_user_id=${encodeURIComponent(
    creatorId
  )}&max_cursor=${encodeURIComponent(String(maxCursor))}&count=${encodeURIComponent(String(count))}`;
}

function buildMixAwemeUrl(mixId, cursor = 0, count = 20) {
  return `https://www.douyin.com/aweme/v1/web/mix/aweme/?mix_id=${encodeURIComponent(
    mixId
  )}&cursor=${encodeURIComponent(String(cursor))}&count=${encodeURIComponent(String(count))}&device_platform=webapp&aid=6383&channel=channel_pc_web`;
}

function normalizeBucketName(value) {
  return cleanText(String(value || '').replace(/^合集\s*[·\-:：]?\s*/u, ''));
}

async function fetchJsonInPage(page, requestUrl) {
  const rawText = await page.evaluate(async (url) => {
    const response = await fetch(url, { credentials: 'include' });
    return await response.text();
  }, requestUrl);
  const normalizedText = cleanText(rawText);
  if (!normalizedText || normalizedText === '{"status_code":0}') {
    return null;
  }
  return JSON.parse(rawText);
}

async function fetchProfilePayload(page, creatorId, notes) {
  if (!creatorId) {
    return null;
  }
  try {
    return await fetchJsonInPage(
      page,
      `https://www.douyin.com/aweme/v1/web/user/profile/other/?sec_user_id=${encodeURIComponent(
        creatorId
      )}&device_platform=webapp&aid=6383&channel=channel_pc_web`
    );
  } catch (error) {
    notes.push(`profile fetch failed: ${cleanText(error && error.message ? error.message : String(error))}`);
    return null;
  }
}

function collectBucketCandidates(awemeList) {
  const buckets = new Map();
  for (const aweme of awemeList) {
    const mixInfo = aweme && aweme.mix_info ? aweme.mix_info : null;
    const seriesInfo = aweme && aweme.series_info ? aweme.series_info : null;
    const mixId = cleanText((mixInfo && mixInfo.mix_id) || (seriesInfo && seriesInfo.series_id));
    const bucketName = normalizeBucketName(
      (mixInfo && mixInfo.mix_name) || (seriesInfo && seriesInfo.series_name) || ''
    );
    if (!mixId || !bucketName) continue;
    const existing = buckets.get(mixId) || {
      bucket_id: mixId,
      bucket_name: bucketName,
      bucket_kind: mixInfo ? 'mix' : 'series',
      video_hint_count: 0,
    };
    const mixStats = mixInfo && mixInfo.statis ? mixInfo.statis : {};
    const seriesStats = seriesInfo && seriesInfo.stats ? seriesInfo.stats : {};
    existing.video_hint_count = Math.max(
      Number(existing.video_hint_count || 0),
      Number(mixStats.updated_to_episode || 0),
      Number(seriesStats.updated_to_episode || 0),
      Number(seriesStats.total_episode || 0)
    );
    buckets.set(mixId, existing);
  }
  return Array.from(buckets.values()).sort(
    (a, b) => (Number(b.video_hint_count || 0) - Number(a.video_hint_count || 0)) || a.bucket_name.localeCompare(b.bucket_name)
  );
}

function selectBucketCandidate(bucketCandidates, categoryTitle, categoryIndex) {
  if (!categoryTitle && categoryIndex < 0) {
    return null;
  }
  if (categoryTitle) {
    const target = cleanText(categoryTitle);
    return (
      bucketCandidates.find((item) => cleanText(item.bucket_name) === target) ||
      bucketCandidates.find((item) => cleanText(item.bucket_name).includes(target))
    );
  }
  if (categoryIndex >= 0 && categoryIndex < bucketCandidates.length) {
    return bucketCandidates[categoryIndex];
  }
  return null;
}

async function fetchCreatorBucketInventory(page, creatorId, fallbackCreator, notes) {
  const awemeList = [];
  let cursor = 0;
  let hasMore = true;
  let rounds = 0;
  const limit = 24;
  while (hasMore && rounds < limit) {
    const payload = await fetchJsonInPage(page, buildCreatorPostUrl(creatorId, cursor, 18)).catch((error) => {
      notes.push(`creator aweme/post fetch failed: ${cleanText(error && error.message ? error.message : String(error))}`);
      return null;
    });
    if (!payload) {
      break;
    }
    const pageAwemeList = Array.isArray(payload.aweme_list) ? payload.aweme_list : [];
    awemeList.push(...pageAwemeList);
    hasMore = Number(payload.has_more || 0) === 1;
    cursor = Number(payload.max_cursor || 0);
    rounds += 1;
    if (!pageAwemeList.length) {
      break;
    }
  }
  if (rounds) {
    notes.push(`captured ${rounds} creator aweme/post api page(s) for bucket discovery`);
  }
  return {
    awemeList,
    bucketCandidates: collectBucketCandidates(awemeList),
  };
}

async function fetchMixInventory(page, mixId, sourceBucket, fallbackCreator, notes) {
  const inventoryMap = new Map();
  let cursor = 0;
  let hasMore = true;
  let rounds = 0;
  const limit = 24;
  while (hasMore && rounds < limit) {
    const payload = await fetchJsonInPage(page, buildMixAwemeUrl(mixId, cursor, 20)).catch((error) => {
      notes.push(`mix aweme fetch failed: ${cleanText(error && error.message ? error.message : String(error))}`);
      return null;
    });
    if (!payload) {
      break;
    }
    const awemeList = Array.isArray(payload.aweme_list) ? payload.aweme_list : [];
    for (const aweme of awemeList) {
      const record = buildNetworkRecord(aweme, fallbackCreator, sourceBucket);
      if (!record) continue;
      record.source_rank = 'api_mix_aweme_bucket';
      record.inventory_notes = cleanText(record.inventory_notes || `source_bucket=${sourceBucket}`);
      inventoryMap.set(record.video_id || record.video_url, record);
    }
    hasMore = Number(payload.has_more || 0) === 1;
    cursor = Number(payload.cursor || payload.max_cursor || 0);
    rounds += 1;
    if (!awemeList.length) {
      break;
    }
  }
  notes.push(`captured ${rounds} mix/aweme api page(s) for bucket ${sourceBucket}`);
  if (!hasMore) {
    notes.push('stopped because mix has_more=false');
  } else if (rounds >= limit) {
    notes.push(`stopped because mix continuation limit ${limit} was reached`);
  }
  return Array.from(inventoryMap.values());
}

async function collectTabCandidates(page) {
  const candidates = await page
    .locator('button, [role="tab"], [role="button"], a, div, span')
    .evaluateAll((nodes) =>
      nodes
        .map((node, locatorIndex) => {
          const text = (node.innerText || node.textContent || '').replace(/\s+/g, ' ').trim();
          const rect = typeof node.getBoundingClientRect === 'function' ? node.getBoundingClientRect() : null;
          const className = typeof node.className === 'string' ? node.className : '';
          const dataset = node.dataset ? JSON.stringify(node.dataset) : '';
          return {
            locatorIndex,
            text,
            role: node.getAttribute('role') || '',
            ariaSelected: node.getAttribute('aria-selected') || '',
            className,
            dataset,
            top: rect ? Math.round(rect.top) : 0,
            left: rect ? Math.round(rect.left) : 0,
            width: rect ? Math.round(rect.width) : 0,
            height: rect ? Math.round(rect.height) : 0,
          };
        })
        .filter((item) => item.text)
        .filter((item) => item.text.length <= 32)
        .filter((item) => item.width >= 30 && item.height >= 16)
        .filter((item) => item.top >= 0 && item.top <= 1200)
        .filter(
          (item) =>
            item.role === 'tab' ||
            item.ariaSelected ||
            /tab|tabs|switch|label|title|nav/i.test(item.className) ||
            /tab|tabs|switch|label|title/i.test(item.dataset)
        )
    );

  const deduped = new Map();
  for (const candidate of candidates) {
    const key = candidate.text;
    const existing = deduped.get(key);
    if (!existing || candidate.top < existing.top || (candidate.top === existing.top && candidate.left < existing.left)) {
      deduped.set(key, candidate);
    }
  }
  return Array.from(deduped.values()).sort((a, b) => (a.top - b.top) || (a.left - b.left));
}

async function selectCategoryTab(page, categoryTitle, categoryIndex, waitMs, notes) {
  const availableTabs = await collectTabCandidates(page);
  if (availableTabs.length) {
    notes.push(`available_tabs=${availableTabs.map((item) => item.text).join(' | ')}`);
  } else {
    notes.push('available_tabs=none_detected');
  }

  if (!categoryTitle && categoryIndex < 0) {
    return {
      selectedBucket: '',
      availableTabs,
      selectionMode: 'default_record_tab',
    };
  }

  let selected = null;
  if (categoryTitle) {
    const target = cleanText(categoryTitle);
    selected =
      availableTabs.find((item) => cleanText(item.text) === target) ||
      availableTabs.find((item) => cleanText(item.text).includes(target));
  } else if (categoryIndex >= 0 && categoryIndex < availableTabs.length) {
    selected = availableTabs[categoryIndex];
  }

  if (!selected) {
    notes.push(
      categoryTitle
        ? `category_title_not_found=${cleanText(categoryTitle)}`
        : `category_index_not_found=${String(categoryIndex)}`
    );
    return {
      selectedBucket: categoryTitle ? cleanText(categoryTitle) : `tab_index_${String(categoryIndex)}`,
      availableTabs,
      selectionMode: categoryTitle ? 'category_title_missing' : 'category_index_missing',
      clicked: false,
    };
  }

  const locator = page.locator('button, [role="tab"], [role="button"], a, div, span').nth(selected.locatorIndex);
  await locator.scrollIntoViewIfNeeded().catch(() => {});
  await locator.click({ force: true }).catch(async () => {
    await page.mouse.click(selected.left + Math.max(8, Math.round(selected.width / 2)), selected.top + Math.max(8, Math.round(selected.height / 2)));
  });
  await page.waitForTimeout(Math.max(waitMs, 1200));

  notes.push(
    categoryTitle
      ? `selected_category_title=${selected.text}`
      : `selected_category_index=${String(categoryIndex)}:${selected.text}`
  );
  return {
    selectedBucket: selected.text,
    availableTabs,
    selectionMode: categoryTitle ? 'category_title' : 'category_index',
    clicked: true,
  };
}

async function run() {
  const url = argValue('--url');
  const outputJson = argValue('--output-json');
  const scrollRounds = Number(argValue('--scroll-rounds', '12'));
  const waitMs = Number(argValue('--wait-ms', '1800'));
  const cookiesFile = argValue('--cookies-file');
  const categoryTitle = cleanText(argValue('--category-title'));
  const categoryIndexRaw = argValue('--category-index', '');
  const categoryIndex = categoryIndexRaw === '' ? -1 : Number(categoryIndexRaw);
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
  let captureEnabled = false;
  let sourceBucket = '';

  page.on('response', (response) => {
    responseTasks.push((async () => {
      if (!captureEnabled) {
        return;
      }
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
          }, sourceBucket);
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
  const profilePayload = await fetchProfilePayload(page, creatorId, notes);
  const profileUser = profilePayload && profilePayload.user ? profilePayload.user : {};
  const bodyInitial = await page.locator('body').innerText().catch(() => '');
  const initialText = cleanText(bodyInitial);
  const nameMatch = bodyInitial.match(/\n([^\n]{1,30})\n关注\n(?:\d+|粉丝)/);
  creatorName =
    cleanText(profileUser.nickname) ||
    (nameMatch ? cleanText(nameMatch[1]) : cleanText((await page.title()).replace(/的抖音\s*-\s*抖音$/, '')));

  if (profilePayload) {
    const mixCount = Number(profileUser.mix_count || 0);
    const seriesCount = Number(profileUser.series_count || 0);
    if (mixCount || seriesCount) {
      notes.push(`profile mix_count=${String(mixCount)} series_count=${String(seriesCount)}`);
    }
  }

  if (initialText.includes('服务异常')) {
    notes.push('creator page shows service exception in works area');
  }
  if (initialText.includes('验证码中间页')) {
    notes.push('creator page blocked by captcha');
  }

  if (categoryTitle || categoryIndex >= 0) {
    const discovery = await fetchCreatorBucketInventory(
      page,
      creatorId,
      {
        creator_id: creatorId,
        creator_name: creatorName,
        creator_home_url: creatorHomeUrl || page.url(),
      },
      notes
    );
    const bucketCandidates = discovery.bucketCandidates;
    if (bucketCandidates.length) {
      notes.push(
        `available_tabs=${bucketCandidates
          .map((item, index) => `${String(index)}:${item.bucket_name}(${String(item.video_hint_count || 0)})`)
          .join(' | ')}`
      );
    } else {
      notes.push('available_tabs=none_detected');
    }

    const selectedBucket = selectBucketCandidate(bucketCandidates, categoryTitle, categoryIndex);
    sourceBucket =
      (selectedBucket && cleanText(selectedBucket.bucket_name)) ||
      (categoryTitle ? categoryTitle : categoryIndex >= 0 ? `tab_index_${String(categoryIndex)}` : 'record');
    const selectionMode = categoryTitle ? 'category_title' : 'category_index';
    if (!selectedBucket) {
      notes.push(
        categoryTitle
          ? `category_title_not_found=${cleanText(categoryTitle)}`
          : `category_index_not_found=${String(categoryIndex)}`
      );
      const emptyResult = {
        input_url: url,
        final_url: page.url(),
        creator_home_url: creatorHomeUrl || page.url(),
        creator_id: creatorId,
        creator_name: creatorName,
        source_bucket: sourceBucket,
        selection_mode: `${selectionMode}_missing`,
        available_tabs: bucketCandidates.map((item) => item.bucket_name),
        discovered_count: 0,
        deduped_count: 0,
        notes,
        records: [],
      };
      if (outputJson) {
        fs.writeFileSync(outputJson, JSON.stringify(emptyResult, null, 2), 'utf8');
      }
      process.stdout.write(JSON.stringify(emptyResult, null, 2));
      await context.close();
      await browser.close();
      return;
    }

    notes.push(
      categoryTitle
        ? `selected_category_title=${selectedBucket.bucket_name}`
        : `selected_category_index=${String(categoryIndex)}:${selectedBucket.bucket_name}`
    );
    const bucketRecords = await fetchMixInventory(
      page,
      selectedBucket.bucket_id,
      selectedBucket.bucket_name,
      {
        creator_id: creatorId,
        creator_name: creatorName,
        creator_home_url: creatorHomeUrl || page.url(),
      },
      notes
    );
    const records = bucketRecords
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
        inventory_notes: cleanText(item.inventory_notes || `source_bucket=${selectedBucket.bucket_name}`),
      }))
      .sort((a, b) => String(a.video_id || a.video_url).localeCompare(String(b.video_id || b.video_url)));
    const result = {
      input_url: url,
      final_url: page.url(),
      creator_home_url: creatorHomeUrl || page.url(),
      creator_id: creatorId,
      creator_name: creatorName,
      source_bucket: selectedBucket.bucket_name,
      selection_mode: selectionMode,
      available_tabs: bucketCandidates.map((item) => item.bucket_name),
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
    return;
  }

  const tabSelection = await selectCategoryTab(page, categoryTitle, categoryIndex, waitMs, notes);
  sourceBucket = cleanText(tabSelection.selectedBucket) || (categoryTitle ? categoryTitle : (categoryIndex >= 0 ? `tab_index_${String(categoryIndex)}` : 'record'));
  inventoryMap.clear();
  networkPageCursorSeen.clear();
  responseTasks.length = 0;
  networkPageCount = 0;
  lastSuccessfulPageUrl = '';
  lastPayloadHasMore = 0;
  lastPayloadMaxCursor = 0;
  captureEnabled = true;

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
        }, sourceBucket);
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
      inventory_notes: cleanText(item.inventory_notes || (sourceBucket ? `source_bucket=${sourceBucket}` : '')),
    }))
    .sort((a, b) => String(a.video_id || a.video_url).localeCompare(String(b.video_id || b.video_url)));

  const result = {
    input_url: url,
    final_url: page.url(),
    creator_home_url: creatorHomeUrl || page.url(),
    creator_id: creatorId,
    creator_name: creatorName,
    source_bucket: sourceBucket,
    selection_mode: tabSelection.selectionMode,
    available_tabs: tabSelection.availableTabs ? tabSelection.availableTabs.map((item) => item.text) : [],
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
