import { ArrowRight, Database, FileText, TerminalSquare } from "lucide-react";
import Link from "next/link";
import { redirect } from "next/navigation";
import { addWaitlistSignup } from "@/lib/waitlist";

type HomePageProps = {
  searchParams?: Promise<{
    waitlist?: string;
  }>;
};

export default async function HomePage({ searchParams }: HomePageProps) {
  const params = await searchParams;
  const waitlistState = params?.waitlist;
  const message =
    waitlistState === "joined"
      ? "Thank you. You're on the waitlist."
      : waitlistState === "exists"
        ? "That email is already on the waitlist."
        : waitlistState === "invalid"
          ? "Enter a valid email."
          : waitlistState === "error"
            ? "Something went wrong. Try again shortly."
            : null;
  const messageClass =
    waitlistState === "joined" || waitlistState === "exists" ? "form-status form-status-success" : "form-status";

  async function joinWaitlist(formData: FormData) {
    "use server";

    let destination = "/?waitlist=error";
    try {
      const result = await addWaitlistSignup(formData.get("email"), "homepage");
      destination = `/?waitlist=${result.created ? "joined" : "exists"}`;
    } catch (error) {
      destination = error instanceof Error && error.message === "INVALID_EMAIL" ? "/?waitlist=invalid" : "/?waitlist=error";
      if (destination.endsWith("error")) {
        console.error(error);
      }
    }
    redirect(destination);
  }

  return (
    <main className="site-shell">
      <section className="landing">
        <p className="eyebrow">California LCAP Intelligence</p>
        <h1>Public school planning data, made usable for GTM.</h1>
        <p className="lede">
          LCAP PDFs, Dashboard outcomes, narrative search, funding signals, and district contacts in one MCP-ready system.
        </p>

        <form className="waitlist-form" action={joinWaitlist}>
          <label className="sr-only" htmlFor="waitlist-email">
            Email
          </label>
          <input id="waitlist-email" name="email" type="email" placeholder="you@company.com" required />
          <button type="submit">
            Join waitlist
            <ArrowRight size={17} />
          </button>
        </form>
        {message ? <p className={messageClass}>{message}</p> : null}

        <p className="report-link">
          <Link href="/chronic-absenteeism-report">
            See an example on chronic absenteeism
            <ArrowRight size={15} />
          </Link>
        </p>

        <div className="landing-facts" aria-label="System summary">
          <div>
            <Database size={18} />
            <span>California LCAP + Dashboard data</span>
          </div>
          <div>
            <FileText size={18} />
            <span>Section-cited narrative evidence</span>
          </div>
          <div>
            <TerminalSquare size={18} />
            <span>Built for Codex and Claude via MCP</span>
          </div>
        </div>
      </section>
    </main>
  );
}
