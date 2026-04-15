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

function extractContentBlock(bodyText) {
  const lines = dedupeLines(
    String(bodyText || '')
      .split(/\r?\n/)
      .map((line) => cleanText(line))
      .filter(Boolean)
  );
  const startMarkers = ['00:00 /', '章节要点', '内容由AI生成'];
  const endMarkers = ['全部评论', '推荐视频', '登录后免费畅享高清视频', '广告投放'];
  let startIndex = lines.findIndex((line) => startMarkers.some((marker) => line.includes(marker)));
  if (startIndex === -1) {
    startIndex = lines.findIndex((line) => line.includes('发布时间：'));
  }
  if (startIndex === -1) {
    return '';
  }
  let endIndex = lines.findIndex(
    (line, idx) => idx > startIndex && endMarkers.some((marker) => line.includes(marker))
  );
  if (endIndex === -1) {
    endIndex = Math.min(lines.length, startIndex + 80);
  }
  return cleanMultiline(lines.slice(startIndex, endIndex).join('\n'));
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
    await page.screenshot({ path: expandedPath });
    result.screenshot_files.push(expandedPath);

    await page.evaluate(() => window.scrollTo(0, Math.max(900, window.innerHeight * 0.8)));
    await page.waitForTimeout(1200);
    const middlePath = path.join(screenshotDir, '03_mid_page.png');
    await page.screenshot({ path: middlePath });
    result.screenshot_files.push(middlePath);

    const bodyText = cleanMultiline(await page.locator('body').innerText().catch(() => ''));
    result.creator_visible = extractCreator(`\n${bodyText}\n`);
    result.publish_date_visible = extractPublishDate(bodyText);
    result.description_visible = extractDescription(bodyText);
    result.page_text_visible = extractContentBlock(bodyText) || bodyText.slice(0, 3000);
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
