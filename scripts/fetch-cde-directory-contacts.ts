import dotenv from "dotenv";
import { neon, type NeonQueryFunction } from "@neondatabase/serverless";
import type { BrowserContext, Page } from "playwright-core";
import { existsSync } from "node:fs";
import { mkdir, readFile } from "node:fs/promises";
import { join } from "node:path";

dotenv.config({ path: ".env.local", quiet: true });
dotenv.config({ path: ".env", quiet: true });

type Sql = NeonQueryFunction<false, false>;
type ContactRole = "superintendent" | "chief_business_official" | "cds_coordinator";

interface ParsedContact {
  role: ContactRole;
  name: string | null;
  title: string | null;
  phone: string | null;
  email: string | null;
}

interface ParsedDirectoryProfile {
  cds_code: string;
  county: string | null;
  district: string | null;
  district_address: string | null;
  mailing_address: string | null;
  phone: string | null;
  fax: string | null;
  email: string | null;
  website: string | null;
  status: string | null;
  district_type: string | null;
  low_grade: string | null;
  high_grade: string | null;
  nces_district_id: string | null;
  cde_detail_url: string;
  cde_last_updated: string | null;
  parse_status: string;
  parse_error: string | null;
  source: string;
}

interface ParsedDirectoryPage {
  profile: ParsedDirectoryProfile;
  contacts: ParsedContact[];
}

interface PublicDistrictRecord {
  cds_code?: string;
  county?: string;
  district?: string;
  status_type?: string;
  doc_type?: string;
  street?: string;
  city?: string;
  zip?: string;
  state?: string;
  mail_street?: string;
  mail_city?: string;
  mail_zip?: string;
  mail_state?: string;
  phone?: string;
  extension?: string;
  fax_number?: string;
  admin_first_name?: string;
  admin_last_name?: string;
  last_update?: string;
}

const DETAIL_URL = "https://www.cde.ca.gov/SchoolDirectory/details?cdscode={cdsCode}";
const DEFAULT_PUBLIC_DISTRICTS_PATH = "data/cde/public_districts.json";
const DEFAULT_BROWSER_PROFILE_DIR = ".cache/cde-directory-browser";
const DEFAULT_CHROME_PATH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";

function argValue(name: string): string | undefined {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

function argValues(name: string): string[] {
  const values: string[] = [];
  process.argv.forEach((arg, index) => {
    if (arg === name && process.argv[index + 1]) {
      values.push(process.argv[index + 1]);
    }
  });
  return values;
}

function hasArg(name: string): boolean {
  return process.argv.includes(name);
}

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing ${name}. Add it to .env.local or export it before running this script.`);
  }
  return value;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function detailUrl(cdsCode: string): string {
  return DETAIL_URL.replace("{cdsCode}", cdsCode);
}

function isCaptchaHtml(html: string): boolean {
  return /Radware Captcha Page|Please solve this CAPTCHA|captcha\.perfdrive\.com/i.test(html);
}

function decodeEntities(value: string): string {
  return value
    .replace(/&nbsp;/gi, " ")
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&#(\d+);/g, (_match, code: string) => String.fromCharCode(Number(code)));
}

function compact(value: string | null | undefined): string | null {
  const cleaned = decodeEntities(value ?? "")
    .replace(/\r/g, "\n")
    .replace(/[ \t]+/g, " ")
    .replace(/\n\s+/g, "\n")
    .trim();
  if (!cleaned || /^information not available$/i.test(cleaned) || /^no data$/i.test(cleaned)) {
    return null;
  }
  return cleaned;
}

function stripHtmlToLines(html: string): string[] {
  return decodeEntities(
    html
      .replace(/<br\s*\/?>/gi, "\n")
      .replace(/<\/(div|p|li|span|a)>/gi, "\n")
      .replace(/<script[\s\S]*?<\/script>/gi, "")
      .replace(/<style[\s\S]*?<\/style>/gi, "")
      .replace(/<[^>]+>/g, "")
  )
    .split("\n")
    .map((line) => line.replace(/\s+/g, " ").trim())
    .filter(Boolean)
    .filter((line) => !/^google map$/i.test(line))
    .filter((line) => !/^link opens new browser tab$/i.test(line))
    .filter((line) => !/^external link opens/i.test(line));
}

function normalizeLabel(label: string): string {
  return stripHtmlToLines(label).join(" ").toLowerCase().replace(/[^a-z0-9]+/g, "");
}

function extractFields(html: string): Map<string, string[]> {
  const fields = new Map<string, string[]>();
  const rowPattern = /<tr[^>]*>\s*<th[^>]*>([\s\S]*?)<\/th>\s*<td[^>]*>([\s\S]*?)<\/td>\s*<\/tr>/gi;
  for (const match of html.matchAll(rowPattern)) {
    const label = normalizeLabel(match[1] ?? "");
    if (label) {
      fields.set(label, stripHtmlToLines(match[2] ?? ""));
    }
  }
  return fields;
}

function linesFor(fields: Map<string, string[]>, ...labels: string[]): string[] {
  const normalized = labels.map((label) => label.toLowerCase().replace(/[^a-z0-9]+/g, ""));
  for (const [key, lines] of fields) {
    if (normalized.some((label) => key === label || key.startsWith(label))) {
      return lines;
    }
  }
  return [];
}

function firstValue(fields: Map<string, string[]>, ...labels: string[]): string | null {
  return compact(linesFor(fields, ...labels)[0]);
}

function multilineValue(fields: Map<string, string[]>, ...labels: string[]): string | null {
  return compact(linesFor(fields, ...labels).join("\n"));
}

function parseCdsCode(fields: Map<string, string[]>, fallback: string): string {
  const value = firstValue(fields, "CDS Code") ?? fallback;
  const digits = value.replace(/\D/g, "");
  return digits || fallback;
}

function cleanContactLine(line: string): string {
  return line
    .replace(/^\(Contact for Data Updates\)\s*/i, "")
    .replace(/^Request Data Update\(s\)$/i, "")
    .trim();
}

function parseContact(role: ContactRole, lines: string[]): ParsedContact | null {
  const cleaned = lines
    .map(cleanContactLine)
    .map((line) => compact(line))
    .filter((line): line is string => Boolean(line));
  const email = cleaned.find((line) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(line)) ?? null;
  const phone = cleaned.find((line) => /\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}/.test(line)) ?? null;
  const nameTitleLines = cleaned.filter((line) => line !== email && line !== phone && line !== "Request Data Update(s)");
  const name = nameTitleLines[0] ?? null;
  const title = nameTitleLines[1] ?? null;
  if (!name && !title && !phone && !email) {
    return null;
  }
  return { role, name, title, phone, email };
}

function formatAddress(street?: string, city?: string, state?: string, zip?: string): string | null {
  const line1 = compact(street);
  const cityState = [compact(city), compact(state)].filter(Boolean).join(", ");
  const cityStateZip = [cityState || null, compact(zip)].filter(Boolean).join(" ");
  return [line1, cityStateZip || null].filter(Boolean).join("\n") || null;
}

function phoneWithExtension(phone?: string, extension?: string): string | null {
  const base = compact(phone);
  const ext = compact(extension);
  if (!base) {
    return null;
  }
  return ext ? `${base} Ext. ${ext}` : base;
}

function publicDistrictToPage(record: PublicDistrictRecord): ParsedDirectoryPage | null {
  const cdsCode = compact(record.cds_code)?.replace(/\D/g, "");
  if (!cdsCode) {
    return null;
  }

  const firstName = compact(record.admin_first_name);
  const lastName = compact(record.admin_last_name);
  const adminName = [firstName, lastName].filter(Boolean).join(" ") || null;
  const districtPhone = phoneWithExtension(record.phone, record.extension);
  const contacts: ParsedContact[] = adminName || districtPhone
    ? [
        {
          role: "superintendent",
          name: adminName,
          title: "District administrator listed by CDE",
          phone: districtPhone,
          email: null
        }
      ]
    : [];

  return {
    profile: {
      cds_code: cdsCode,
      county: compact(record.county),
      district: compact(record.district),
      district_address: formatAddress(record.street, record.city, record.state, record.zip),
      mailing_address: formatAddress(record.mail_street, record.mail_city, record.mail_state, record.mail_zip),
      phone: districtPhone,
      fax: compact(record.fax_number),
      email: null,
      website: null,
      status: compact(record.status_type),
      district_type: compact(record.doc_type),
      low_grade: null,
      high_grade: null,
      nces_district_id: null,
      cde_detail_url: detailUrl(cdsCode),
      cde_last_updated: compact(record.last_update),
      parse_status: "ok",
      parse_error: null,
      source: "cde_public_districts_file"
    },
    contacts
  };
}

export function parseDirectoryHtml(html: string, fallbackCdsCode: string): ParsedDirectoryPage {
  if (isCaptchaHtml(html)) {
    throw new Error("CDE returned a captcha page instead of directory details.");
  }
  if (!/California School Directory/i.test(html)) {
    throw new Error("Response does not look like a CDE School Directory detail page.");
  }

  const fields = extractFields(html);
  const cdsCode = parseCdsCode(fields, fallbackCdsCode);
  const sourceUrl = detailUrl(cdsCode);
  const contacts = [
    parseContact("superintendent", linesFor(fields, "Superintendent")),
    parseContact("chief_business_official", linesFor(fields, "Chief Business Official")),
    parseContact("cds_coordinator", linesFor(fields, "CDS Coordinator"))
  ].filter((contact): contact is ParsedContact => Boolean(contact));

  return {
    profile: {
      cds_code: cdsCode,
      county: firstValue(fields, "County"),
      district: firstValue(fields, "District"),
      district_address: multilineValue(fields, "District Address"),
      mailing_address: multilineValue(fields, "Mailing Address"),
      phone: firstValue(fields, "Phone Number"),
      fax: firstValue(fields, "Fax Number"),
      email: firstValue(fields, "Email"),
      website: firstValue(fields, "Web Address"),
      status: firstValue(fields, "Status"),
      district_type: firstValue(fields, "District Type"),
      low_grade: firstValue(fields, "Low Grade"),
      high_grade: firstValue(fields, "High Grade"),
      nces_district_id: firstValue(fields, "NCES/Federal District ID"),
      cde_detail_url: sourceUrl,
      cde_last_updated: firstValue(fields, "Last Updated"),
      parse_status: "ok",
      parse_error: null,
      source: "cde_school_directory_detail"
    },
    contacts
  };
}

async function ensureSchema(sql: Sql) {
  await sql.query(`
    create table if not exists district_directory_profiles (
      cds_code text primary key,
      county text,
      district text,
      district_address text,
      mailing_address text,
      phone text,
      fax text,
      email text,
      website text,
      status text,
      district_type text,
      low_grade text,
      high_grade text,
      nces_district_id text,
      cde_detail_url text,
      cde_last_updated text,
      fetched_at timestamptz not null default now(),
      parse_status text,
      parse_error text,
      source text
    )
  `);
  await sql.query(`
    create table if not exists district_directory_contacts (
      cds_code text not null references district_directory_profiles(cds_code) on delete cascade,
      role text not null,
      name text,
      title text,
      phone text,
      email text,
      source text,
      fetched_at timestamptz not null default now(),
      primary key (cds_code, role)
    )
  `);
  await sql.query("create index if not exists idx_directory_profiles_district on district_directory_profiles(district)");
  await sql.query("create index if not exists idx_directory_profiles_county on district_directory_profiles(county)");
  await sql.query("create index if not exists idx_directory_contacts_role on district_directory_contacts(role)");
}

async function selectCdsCodes(sql: Sql, limit: number, startAfter?: string): Promise<string[]> {
  const params: unknown[] = [];
  const filters = ["d.cds_code is not null", "d.cds_code like '%0000000'", "coalesce(d.status_type, 'Active') = 'Active'"];
  if (startAfter) {
    params.push(startAfter);
    filters.push(`d.cds_code > $${params.length}`);
  }
  params.push(limit);
  const rows = (await sql.query(
    `
      select distinct d.cds_code
      from districts d
      where ${filters.join(" and ")}
        and (coalesce(d.has_lcap, 0) = 1 or coalesce(d.has_dashboard, 0) = 1)
      order by d.cds_code
      limit $${params.length}
    `,
    params
  )) as Array<{ cds_code: string }>;
  return rows.map((row) => row.cds_code);
}

async function selectMissingDetailCdsCodes(sql: Sql, limit: number, startAfter?: string): Promise<string[]> {
  const params: unknown[] = [];
  const filters = ["d.cds_code is not null", "d.cds_code like '%0000000'", "coalesce(d.status_type, 'Active') = 'Active'"];
  if (startAfter) {
    params.push(startAfter);
    filters.push(`d.cds_code > $${params.length}`);
  }
  params.push(limit);
  const rows = (await sql.query(
    `
      select distinct d.cds_code
      from districts d
      left join district_directory_profiles p on p.cds_code = d.cds_code
      where ${filters.join(" and ")}
        and (coalesce(d.has_lcap, 0) = 1 or coalesce(d.has_dashboard, 0) = 1)
        and not (
          p.source = 'cde_school_directory_detail'
          and p.parse_status = 'ok'
        )
      order by d.cds_code
      limit $${params.length}
    `,
    params
  )) as Array<{ cds_code: string }>;
  return rows.map((row) => row.cds_code);
}

async function fetchHtml(cdsCode: string): Promise<string> {
  const response = await fetch(detailUrl(cdsCode), {
    headers: {
      "user-agent":
        "Mozilla/5.0 (compatible; ca-lcap-intelligence/0.1; +https://ca-lcap-intelligence.vercel.app)"
    }
  });
  const html = await response.text();
  if (!response.ok) {
    throw new Error(`CDE request failed with HTTP ${response.status}.`);
  }
  return html;
}

async function htmlForCdsCode(cdsCode: string, htmlDir?: string): Promise<string> {
  if (htmlDir) {
    return readFile(join(htmlDir, `${cdsCode}.html`), "utf8");
  }
  return fetchHtml(cdsCode);
}

async function createBrowserContext({
  userDataDir,
  headless,
  chromePath
}: {
  userDataDir: string;
  headless: boolean;
  chromePath?: string;
}): Promise<BrowserContext> {
  await mkdir(userDataDir, { recursive: true });
  const { chromium } = await import("playwright-core");
  return chromium.launchPersistentContext(userDataDir, {
    headless,
    executablePath: chromePath || undefined,
    viewport: { width: 1280, height: 900 },
    userAgent:
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
  });
}

async function htmlForCdsCodeWithBrowser({
  page,
  cdsCode,
  captchaWaitMs,
  postLoadMs,
  headless
}: {
  page: Page;
  cdsCode: string;
  captchaWaitMs: number;
  postLoadMs: number;
  headless: boolean;
}): Promise<string> {
  await page.goto(detailUrl(cdsCode), { waitUntil: "domcontentloaded", timeout: 60_000 });
  if (postLoadMs > 0) {
    await page.waitForTimeout(postLoadMs);
  }

  let html = await page.content();
  if (!isCaptchaHtml(html) || headless || captchaWaitMs <= 0) {
    return html;
  }

  console.warn(
    `captcha detected for ${cdsCode}; solve it in the opened browser, then this job will continue automatically`
  );
  const deadline = Date.now() + captchaWaitMs;
  while (Date.now() < deadline) {
    await page.waitForTimeout(2_000);
    html = await page.content();
    if (!isCaptchaHtml(html)) {
      return html;
    }
  }

  return html;
}

async function upsertParsedPage(sql: Sql, page: ParsedDirectoryPage) {
  const profile = page.profile;
  await sql.query(
    `
      insert into district_directory_profiles (
        cds_code, county, district, district_address, mailing_address, phone, fax,
        email, website, status, district_type, low_grade, high_grade,
        nces_district_id, cde_detail_url, cde_last_updated, fetched_at,
        parse_status, parse_error, source
      )
      values (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15, $16, now(), $17, $18, $19
      )
      on conflict (cds_code) do update set
        county = excluded.county,
        district = excluded.district,
        district_address = excluded.district_address,
        mailing_address = excluded.mailing_address,
        phone = excluded.phone,
        fax = excluded.fax,
        email = excluded.email,
        website = excluded.website,
        status = excluded.status,
        district_type = excluded.district_type,
        low_grade = excluded.low_grade,
        high_grade = excluded.high_grade,
        nces_district_id = excluded.nces_district_id,
        cde_detail_url = excluded.cde_detail_url,
        cde_last_updated = excluded.cde_last_updated,
        fetched_at = now(),
        parse_status = excluded.parse_status,
        parse_error = excluded.parse_error,
        source = excluded.source
    `,
    [
      profile.cds_code,
      profile.county,
      profile.district,
      profile.district_address,
      profile.mailing_address,
      profile.phone,
      profile.fax,
      profile.email,
      profile.website,
      profile.status,
      profile.district_type,
      profile.low_grade,
      profile.high_grade,
      profile.nces_district_id,
      profile.cde_detail_url,
      profile.cde_last_updated,
      profile.parse_status,
      profile.parse_error,
      profile.source
    ]
  );

  await sql.query("delete from district_directory_contacts where cds_code = $1", [profile.cds_code]);
  for (const contact of page.contacts) {
    await sql.query(
      `
        insert into district_directory_contacts (
          cds_code, role, name, title, phone, email, source, fetched_at
        )
        values ($1, $2, $3, $4, $5, $6, $7, now())
      `,
      [profile.cds_code, contact.role, contact.name, contact.title, contact.phone, contact.email, profile.source]
    );
  }
}

async function upsertPublicDistrictPage(sql: Sql, page: ParsedDirectoryPage) {
  const profile = page.profile;
  await sql.query(
    `
      insert into district_directory_profiles (
        cds_code, county, district, district_address, mailing_address, phone, fax,
        email, website, status, district_type, low_grade, high_grade,
        nces_district_id, cde_detail_url, cde_last_updated, fetched_at,
        parse_status, parse_error, source
      )
      values (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15, $16, now(), $17, $18, $19
      )
      on conflict (cds_code) do update set
        county = excluded.county,
        district = excluded.district,
        district_address = excluded.district_address,
        mailing_address = excluded.mailing_address,
        phone = excluded.phone,
        fax = excluded.fax,
        status = excluded.status,
        district_type = excluded.district_type,
        cde_detail_url = excluded.cde_detail_url,
        cde_last_updated = excluded.cde_last_updated,
        fetched_at = now(),
        parse_status = excluded.parse_status,
        parse_error = excluded.parse_error,
        source = excluded.source
      where not (
        district_directory_profiles.source = 'cde_school_directory_detail'
        and district_directory_profiles.parse_status = 'ok'
      )
    `,
    [
      profile.cds_code,
      profile.county,
      profile.district,
      profile.district_address,
      profile.mailing_address,
      profile.phone,
      profile.fax,
      profile.email,
      profile.website,
      profile.status,
      profile.district_type,
      profile.low_grade,
      profile.high_grade,
      profile.nces_district_id,
      profile.cde_detail_url,
      profile.cde_last_updated,
      profile.parse_status,
      profile.parse_error,
      profile.source
    ]
  );

  for (const contact of page.contacts) {
    await sql.query(
      `
        insert into district_directory_contacts (
          cds_code, role, name, title, phone, email, source, fetched_at
        )
        values ($1, $2, $3, $4, $5, $6, $7, now())
        on conflict (cds_code, role) do update set
          name = excluded.name,
          title = excluded.title,
          phone = excluded.phone,
          email = excluded.email,
          source = excluded.source,
          fetched_at = now()
        where district_directory_contacts.source is distinct from 'cde_school_directory_detail'
      `,
      [profile.cds_code, contact.role, contact.name, contact.title, contact.phone, contact.email, profile.source]
    );
  }
}

async function upsertFailure(sql: Sql, cdsCode: string, error: unknown) {
  const message = error instanceof Error ? error.message : "Unknown fetch/parse error.";
  await sql.query(
    `
      insert into district_directory_profiles (
        cds_code, cde_detail_url, fetched_at, parse_status, parse_error, source
      )
      values ($1, $2, now(), $3, $4, $5)
      on conflict (cds_code) do update set
        cde_detail_url = excluded.cde_detail_url,
        fetched_at = now(),
        parse_status = excluded.parse_status,
        parse_error = excluded.parse_error,
        source = excluded.source
      where district_directory_profiles.parse_status is distinct from 'ok'
    `,
    [
      cdsCode,
      detailUrl(cdsCode),
      message.toLowerCase().includes("captcha") ? "blocked" : "error",
      message.slice(0, 500),
      "cde_school_directory_detail"
    ]
  );
}

async function importPublicDistricts(sql: Sql, path: string, limit: number, dryRun: boolean, includeInactive: boolean) {
  const raw = await readFile(path, "utf8");
  const records = JSON.parse(raw) as PublicDistrictRecord[];
  const pages = records
    .filter((record) => includeInactive || compact(record.status_type) === "Active")
    .map(publicDistrictToPage)
    .filter((page): page is ParsedDirectoryPage => Boolean(page))
    .slice(0, limit);

  console.log(`processing ${pages.length.toLocaleString()} public district records from ${path}`);

  let ok = 0;
  for (const [index, page] of pages.entries()) {
    if (!dryRun) {
      await upsertPublicDistrictPage(sql, page);
    }
    ok += 1;
    if ((index + 1) % 100 === 0 || index === pages.length - 1) {
      console.log(`[${index + 1}/${pages.length}] imported public district contact baseline`);
    }
  }

  console.log(`complete: ${ok.toLocaleString()} public district records imported`);
}

async function main() {
  const sql = neon(requiredEnv("DATABASE_URL"));
  const dryRun = hasArg("--dry-run");
  const reset = hasArg("--reset");
  const strict = hasArg("--strict");
  const useBrowser = hasArg("--browser");
  const missingDetailOnly = hasArg("--missing-detail-only");
  const headless = hasArg("--headless") && !hasArg("--headful");
  const htmlDir = argValue("--html-dir");
  const publicDistrictsPath = argValue("--from-public-districts") ?? (hasArg("--public-districts") ? DEFAULT_PUBLIC_DISTRICTS_PATH : undefined);
  const includeInactive = hasArg("--include-inactive");
  const limit = hasArg("--all") ? 10_000 : Number(argValue("--limit") ?? 25);
  const delayMs = Number(argValue("--delay-ms") ?? (useBrowser ? 3_000 : 750));
  const captchaWaitMs = Number(argValue("--captcha-wait-ms") ?? (useBrowser ? 300_000 : 0));
  const postLoadMs = Number(argValue("--post-load-ms") ?? (useBrowser ? 1_000 : 0));
  const userDataDir = argValue("--user-data-dir") ?? DEFAULT_BROWSER_PROFILE_DIR;
  const chromePathArg = argValue("--chrome-path");
  const chromePath = chromePathArg ?? (existsSync(DEFAULT_CHROME_PATH) ? DEFAULT_CHROME_PATH : undefined);
  const startAfter = argValue("--start-after");
  const explicitCodes = argValues("--cds-code").map((code) => code.replace(/\D/g, ""));

  await ensureSchema(sql);
  if (reset && !dryRun) {
    await sql.query("truncate table district_directory_contacts, district_directory_profiles");
    console.log("reset district directory contact tables");
  }

  if (publicDistrictsPath) {
    await importPublicDistricts(sql, publicDistrictsPath, limit, dryRun, includeInactive);
    return;
  }

  const cdsCodes = explicitCodes.length
    ? explicitCodes
    : missingDetailOnly
      ? await selectMissingDetailCdsCodes(sql, limit, startAfter)
      : await selectCdsCodes(sql, limit, startAfter);
  console.log(`processing ${cdsCodes.length.toLocaleString()} district directory pages`);

  let browserContext: BrowserContext | null = null;
  let browserPage: Page | null = null;
  if (useBrowser && !htmlDir && cdsCodes.length > 0) {
    console.log(
      `using browser-backed detail fetch (${headless ? "headless" : "headful"}, profile: ${userDataDir})`
    );
    browserContext = await createBrowserContext({ userDataDir, headless, chromePath });
    browserPage = await browserContext.newPage();
  }

  let ok = 0;
  let failed = 0;
  try {
    for (const [index, cdsCode] of cdsCodes.entries()) {
      try {
        const html = browserPage
          ? await htmlForCdsCodeWithBrowser({
              page: browserPage,
              cdsCode,
              captchaWaitMs,
              postLoadMs,
              headless
            })
          : await htmlForCdsCode(cdsCode, htmlDir);
        const page = parseDirectoryHtml(html, cdsCode);
        if (!dryRun) {
          await upsertParsedPage(sql, page);
        }
        ok += 1;
        console.log(
          `[${index + 1}/${cdsCodes.length}] ${page.profile.district ?? cdsCode}: ${page.contacts.length} contacts`
        );
      } catch (error) {
        failed += 1;
        if (!dryRun) {
          await upsertFailure(sql, cdsCode, error);
        }
        console.error(`[${index + 1}/${cdsCodes.length}] ${cdsCode}: ${error instanceof Error ? error.message : error}`);
        if (strict) {
          throw error;
        }
      }
      if (!htmlDir && delayMs > 0 && index < cdsCodes.length - 1) {
        await sleep(delayMs);
      }
    }
  } finally {
    await browserContext?.close();
  }

  console.log(`complete: ${ok.toLocaleString()} ok, ${failed.toLocaleString()} failed`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
