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

function cleanMultiline(value) {
  return String(value || '')
    .split(/\r?\n/)
    .map((line) => cleanText(line))
    .filter(Boolean)
    .join('\n');
}

function dedupeLines(lines) {
  const result = [];
  for (const line of lines) {
    if (!line) continue;
    if (!result.length || result[result.length - 1] !== line) {
      result.push(line);
    }
  }
  return result;
}

const NOISE_MARKERS = [
  '合集',
  '播放中',
  '推荐视频',
  '全部评论',
  '大家都在搜',
  '扫码登录',
  '验证码登录',
  '登录后免费畅享高清视频',
  '打开「抖音APP」',
  '广告投放',
  '点击加载更',
  '推荐',
];

function stableAnchor(value) {
  const text = cleanText(value).replace(/#\S+/g, ' ').replace(/\s+/g, ' ').trim();
  if (!text) return '';
  return text.slice(0, Math.min(text.length, 20));
}

function truncateAtNoise(lines) {
  const result = [];
  for (const line of lines) {
    if (!line) continue;
    if (NOISE_MARKERS.some((marker) => line.includes(marker))) {
      break;
    }
    result.push(line);
  }
  return result;
}

function extractPublishDate(bodyText) {
  const match = bodyText.match(/发布时间[:：]\s*([0-9]{4}-[0-9]{2}-[0-9]{2}\s*[0-9]{2}:[0-9]{2})/);
  return match ? cleanText(match[1]) : '';
}

function extractCreator(bodyText) {
  const match = bodyText.match(/\n([^\n]{1,40})\n\s*粉丝[^\n]*获赞/);
  return match ? cleanText(match[1]) : '';
}

function extractDescription(bodyText) {
  const aiMatch = bodyText.match(/内容由AI生成\s*\n([^\n]+)/);
  if (aiMatch) {
    return cleanText(aiMatch[1]);
  }
  const publishMatch = bodyText.match(/\n([^\n]{8,160})\n\d+\n\d+\n\d+\n\d+\n举报\s*\n发布时间[:：]/);
  if (publishMatch) {
    return cleanText(publishMatch[1]);
  }
  return '';
}

function extractContentBlock(bodyText, descriptionText = '', visibleTitle = '') {
  const lines = dedupeLines(
    String(bodyText || '')
      .split(/\r?\n/)
      .map((line) => cleanText(line))
      .filter(Boolean)
  );
  const startMarkers = ['章节要点', '内容由AI生成', '发布时间：', '00:00 /', '|| 00:'];
  const anchors = [stableAnchor(descriptionText), stableAnchor(visibleTitle)].filter(Boolean);

  let anchorIndex = -1;
  for (const anchor of anchors) {
    anchorIndex = lines.findIndex((line) => line.includes(anchor));
    if (anchorIndex !== -1) break;
  }

  let startIndex = anchorIndex;
  if (startIndex === -1) {
    startIndex = lines.findIndex((line) => startMarkers.some((marker) => line.includes(marker)));
  }
  if (startIndex === -1) {
    return '';
  }

  if (anchorIndex !== -1) {
    startIndex = Math.max(0, anchorIndex - 4);
  }

  let candidateLines = lines.slice(startIndex, Math.min(lines.length, startIndex + 40));
  candidateLines = truncateAtNoise(candidateLines);
  return cleanMultiline(candidateLines.join('\n'));
}

async function maybeClickExpand(page) {
  const labels = ['展开', '更多', '全文', '显示更多'];
  for (const label of labels) {
    const locator = page.getByText(label, { exact: true }).first();
    try {
      if (await locator.isVisible({ timeout: 1500 })) {
        await locator.click({ timeout: 2000 });
        await page.waitForTimeout(1200);
        return { clicked: true, label };
      }
    } catch (_) {
      // Ignore and keep trying other labels.
    }
  }
  return { clicked: false, label: '' };
}

async function hideIntrusiveOverlays(page) {
  await page.evaluate(() => {
    const markers = ['登录后免费畅享高清视频', '扫码登录', '验证码登录', '密码登录', '打开「抖音APP」'];
    const elements = Array.from(document.querySelectorAll('body *'));
    for (const element of elements) {
      const text = (element.textContent || '').replace(/\s+/g, ' ').trim();
      if (!text) continue;
      if (!markers.some((marker) => text.includes(marker))) continue;
      let current = element;
      for (let depth = 0; depth < 5 && current; depth += 1) {
        const style = window.getComputedStyle(current);
        if (style.position === 'fixed' || style.position === 'sticky') {
          current.style.display = 'none';
          break;
        }
        current = current.parentElement;
      }
    }
  }).catch(() => {});
}

async function seekVideoToRatio(page, ratio) {
  return page.evaluate((targetRatio) => {
    const video = document.querySelector('video');
    if (!video || !Number.isFinite(video.duration) || video.duration <= 0) {
      return { sought: false, duration: Number(video && video.duration) || 0 };
    }
    const clamped = Math.max(0, Math.min(1, Number(targetRatio) || 0));
    video.currentTime = Math.min(video.duration - 0.2, Math.max(0.1, video.duration * clamped));
    return { sought: true, duration: video.duration, currentTime: video.currentTime };
  }, ratio).catch(() => ({ sought: false, duration: 0 }));
}

async function captureVideoOrPage(page, outputPath) {
  const video = page.locator('video').first();
  try {
    if (await video.count()) {
      await video.screenshot({ path: outputPath });
      return 'video';
    }
  } catch (_) {
    // fall back to the page screenshot below
  }
  await page.screenshot({ path: outputPath });
  return 'page';
}

async function capture() {
  const url = argValue('--url');
  const assetDir = argValue('--asset-dir');
  const outputJson = argValue('--output-json');
  const timeoutSeconds = Number(argValue('--timeout-seconds', '45'));
  if (!url || !assetDir) {
    throw new Error('Missing --url or --asset-dir');
  }

  const screenshotDir = path.join(assetDir, 'playwright');
  fs.mkdirSync(screenshotDir, { recursive: true });

  const browser = await chromium.launch({ channel: 'chrome', headless: true });
  const context = await browser.newContext({
    locale: 'zh-CN',
    viewport: { width: 1440, height: 2200 },
  });
  const page = await context.newPage();
  page.setDefaultTimeout(timeoutSeconds * 1000);

  const result = {
    page_opened: false,
    final_url: '',
    page_title: '',
    visible_title: '',
    creator_visible: '',
    publish_date_visible: '',
    description_visible: '',
    page_text_visible: '',
    screenshot_dir: screenshotDir,
    screenshot_files: [],
    screenshot_count: 0,
    expand_clicked: false,
    expand_label: '',
    notes: [],
  };

  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: timeoutSeconds * 1000 });
    await page.waitForTimeout(5000);
    result.page_opened = true;
    result.final_url = page.url();
    result.page_title = cleanText(await page.title());
    result.visible_title = cleanText(result.page_title.replace(/\s*-\s*抖音$/, ''));

    const firstPath = path.join(screenshotDir, '01_first_screen.png');
    await page.screenshot({ path: firstPath });
    result.screenshot_files.push(firstPath);

    const expandResult = await maybeClickExpand(page);
    result.expand_clicked = expandResult.clicked;
    result.expand_label = expandResult.label;
    if (expandResult.clicked) {
      result.notes.push(`expand clicked:${expandResult.label}`);
    } else {
      result.notes.push('expand not found');
    }

    const expandedPath = path.join(screenshotDir, '02_expanded_text.png');
    await hideIntrusiveOverlays(page);
    await page.screenshot({ path: expandedPath });
    result.screenshot_files.push(expandedPath);

    const seekResult = await seekVideoToRatio(page, 0.55);
    if (seekResult && seekResult.sought) {
      result.notes.push(`video_seeked=${String(Math.round((seekResult.currentTime || 0) * 10) / 10)}s`);
      await page.waitForTimeout(1600);
    } else {
      await page.evaluate(() => window.scrollTo(0, Math.max(900, window.innerHeight * 0.8)));
      await page.waitForTimeout(1200);
    }
    await hideIntrusiveOverlays(page);
    const middlePath = path.join(screenshotDir, '03_mid_page.png');
    const middleMode = await captureVideoOrPage(page, middlePath);
    result.notes.push(`mid_capture_mode=${middleMode}`);
    result.screenshot_files.push(middlePath);

    const bodyText = cleanMultiline(await page.locator('body').innerText().catch(() => ''));
    result.creator_visible = extractCreator(`\n${bodyText}\n`);
    result.publish_date_visible = extractPublishDate(bodyText);
    result.description_visible = extractDescription(bodyText);
    result.page_text_visible =
      extractContentBlock(bodyText, result.description_visible, result.visible_title) ||
      extractContentBlock(bodyText, '', result.visible_title) ||
      cleanMultiline(bodyText.slice(0, 1200));
    result.screenshot_count = result.screenshot_files.length;
  } finally {
    await context.close();
    await browser.close();
  }

  if (outputJson) {
    fs.writeFileSync(outputJson, JSON.stringify(result, null, 2), 'utf8');
  }
  process.stdout.write(JSON.stringify(result, null, 2));
}

capture().catch((error) => {
  const payload = {
    page_opened: false,
    final_url: '',
    page_title: '',
    visible_title: '',
    creator_visible: '',
    publish_date_visible: '',
    description_visible: '',
    page_text_visible: '',
    screenshot_dir: '',
    screenshot_files: [],
    screenshot_count: 0,
    expand_clicked: false,
    expand_label: '',
    notes: [cleanText(error && error.message ? error.message : String(error))],
  };
  process.stdout.write(JSON.stringify(payload, null, 2));
  process.exit(1);
});
