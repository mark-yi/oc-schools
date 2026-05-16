import { getSql } from "./db";

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

export type WaitlistResult = {
  email: string;
  created: boolean;
};

let waitlistTableReady: Promise<void> | null = null;

function normalizeEmail(email: FormDataEntryValue | string | null): string {
  return String(email ?? "").trim().toLowerCase();
}

export async function ensureWaitlistTable() {
  waitlistTableReady ??= (async () => {
    await getSql().query(`
      create table if not exists waitlist_signups (
        email text primary key,
        source text,
        created_at timestamptz not null default now(),
        updated_at timestamptz not null default now()
      )
    `);
    await getSql().query("create index if not exists idx_waitlist_signups_created_at on waitlist_signups(created_at)");
  })().catch((error) => {
    waitlistTableReady = null;
    throw error;
  });
  await waitlistTableReady;
}

export async function addWaitlistSignup(emailInput: FormDataEntryValue | string | null, source = "home"): Promise<WaitlistResult> {
  const email = normalizeEmail(emailInput);
  if (!EMAIL_RE.test(email) || email.length > 254) {
    throw new Error("INVALID_EMAIL");
  }

  await ensureWaitlistTable();

  const [row] = (await getSql().query(
    `
      with inserted as (
        insert into waitlist_signups (email, source)
        values ($1, $2)
        on conflict (email) do nothing
        returning email, true as created
      )
      select email, created from inserted
      union all
      select email, false as created
      from waitlist_signups
      where email = $1
        and not exists (select 1 from inserted)
      limit 1
    `,
    [email, source]
  )) as Array<{ email: string; created: boolean }>;

  return { email: row.email, created: row.created };
}
