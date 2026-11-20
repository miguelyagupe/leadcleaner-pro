# LeadCleaner Pro

For a concise project summary, verified code evidence and sharing limits, see [PORTFOLIO.md](PORTFOLIO.md).

## Setup instructions

## V1 release status

The complete V1 workflow is implemented and covered by the automated test
suite. Every pull request now runs the full suite in GitHub Actions. Production
configuration is declared in `render.yaml`; `.env.example` is the authoritative
inventory of required and optional settings and contains no usable secrets.

## First Time Setup (do this once)

Open your terminal and run these commands one by one:

```bash
# 1. Go into the project folder
cd leadcleaner

# 2. Install required packages
pip3 install flask pandas openpyxl

# 3. Start the app
python3 app.py
```

Then open your browser and go to:
**http://localhost:5000**

---

## Every Time You Want to Use It

```bash
cd leadcleaner
python3 app.py
```

Open browser → **http://localhost:5000**

---

## How to Use

1. Upload your Excel list (.xlsx or .csv)
2. Set the Tax Year you want to target (default: 2022)
3. Click **Process My List**
4. Download your clean list

---

## What the tool does automatically

- ✅ Keeps only leads from the selected tax year
- ✅ Separates prequalified, review, and excluded records with a reason on every row
- ✅ Identifies Tulsa business personal property from the county legal description
- ✅ Removes LLCs, corporations, cannabis businesses, government, and nonprofits
- ✅ Keeps TRUST / TRUSTEE names (family trusts are valid leads)
- ✅ Sorts by Total Due (highest debt first = most motivated sellers)
- ✅ Produces preliminary scores without treating absentee or estate text as proof of death
- ✅ Separates deceased evidence, absentee opportunities, and data-quality review
- ✅ Shows exactly how many records were excluded and why
- ✅ Holds qualification runs for review before any CRM import
- ✅ Builds Leads and Research Queue workspaces
- ✅ Saves pipeline status, priority, research decisions, contacts and notes

## Qualification workbook

Every processing run creates five auditable sheets:

- `Prequalified - Verify`
- `Needs Review`
- `Deceased Research`
- `Absentee Opportunities`
- `Excluded Records`

The current Tulsa source does not include a row-level tax-year column. The
selected year is therefore recorded as import provenance instead of being
misrepresented as a row-level validation.

## Tulsa County Assessor verification

After qualification, the dashboard can verify current ownership in controlled
batches of 25. Each official response is cached in PostgreSQL/SQLite so a
repeated run does not create another county request. The verified export adds:

- current Assessor owner
- official account type
- vacant indicator
- owner-match decision and reason
- source URL and checked timestamp

Request failures, missing pages, and parser failures remain `Not verified`.
They are never converted into exclusions. Commercial or changed-owner records
go to review; only matching Residential/Agricultural records become verified
candidates.

## Death and probate evidence ledger

Research evidence is stored separately from county-data signals. Every item
records its type, outcome, confidence, identity match, source, URL, case number,
subject, event date, notes, and timestamp.

`Confirmed deceased` requires an exact identity match plus confirmed official
probate, death-index, or death-certificate evidence. Obituaries, estate text,
skip-trace mismatches, and probable identity matches can prioritize research,
but cannot confirm death. Confirmed living evidence marks a false positive;
conflicting confirmed evidence remains unresolved.

OSCN pages are saved as evidence links because OSCN itself warns that online
docket information is not an official record. OK2Explore is useful but only
indexes Oklahoma deaths at least five years old, so a missing result is never
treated as evidence that a person is living.

## Approved CRM import

Assessor verification does not write to the CRM automatically. The dashboard
first produces an approval preview with the exact eligible count and total
delinquent debt. Only rows marked `Verified candidate` can be committed.

The commit requires the preview's SHA-256 approval token plus the explicit
confirmation phrase. This prevents a stale preview from approving a changed
workbook. Review, unresolved, and unchecked rows remain outside the CRM.
Repeated commits are idempotent: existing source identities are skipped and
reported instead of duplicated. The current Assessor owner becomes the CRM
owner while the complete source row remains in the audit JSON.

## Calls and daily follow-ups

Every lead supports structured inbound and outbound call logs. Each call stores
the outcome, phone used, duration, notes, timestamp, and next follow-up. The
outcome advances the pipeline automatically:

- `No answer` and `Voicemail left` → Attempted contact
- `Spoke — follow up` → Interested
- `Appointment set` → Appointment scheduled
- `Offer requested` → Negotiation
- `Deal pending` → Contract pending
- `Not interested` → Disqualified

`Call later` is a first-class outcome and requires a future follow-up date.
Marking a lead `Disqualified` clears open follow-ups. A phone marked `Do Not
Contact` is removed from the primary contact slot, clears its follow-up, and
cannot be used to log another call.

Every scheduled follow-up can be opened as a prefilled Google Calendar event
or downloaded as a standards-compliant `.ics` file for Outlook, Apple Calendar,
or another calendar provider. Calendar export is explicit and read-only: the CRM
remains the source of truth, and no third-party credentials are stored.

## Contact ledger

Approved county-list contacts and enrichment results are stored as sourced
phone or email records rather than silently replacing one another. Each record
keeps its source, confidence, label, notes, normalized identity, operational
status, and primary-contact decision. Conflicts remain visible for manual
resolution; only an active primary value is exposed to calling and pipeline
workflows.

Contact values can be marked `Invalid` or `Do Not Contact`. Phone and email
formats are validated at the API boundary, duplicate normalized values are
consolidated, and changing the primary value retains the full history.

Outcomes that require another action cannot be saved without a follow-up date.
The `Today` workspace lists active follow-ups due today or overdue, oldest
first. Call history remains attached to the lead even when the lead later
changes status.

## Visual acquisition pipeline

The Pipeline workspace groups active opportunities into nine operational
stages from `New` through `Closed`. Each column shows its lead count and total
delinquent debt; cards show owner, property, priority, phone readiness, debt,
and follow-up date.

Daryl can move a card by drag-and-drop or with its accessible stage selector.
Every move uses the normal CRM status update path and is written to the lead's
activity history. Large stages show the highest-priority 50 cards while keeping
the complete stage count and debt total visible.

## Property intelligence workspace

The Properties workspace consolidates repeated county-list appearances into
one property card, using Tax ID as the primary identity and property address
only when Tax ID is unavailable. It keeps the number of source records and tax
years visible, but shows debt and workflow data from the latest operational
record so repeated imports do not inflate opportunity totals.

Each card connects the current owner and Assessor check with active evidence,
call count, pipeline stage, and the full lead workspace. Search and stage
filters operate on the consolidated property list.

## Acquisition intelligence

The Reports workspace turns current CRM facts into an operational acquisition
brief: active delinquent debt, contact readiness, due and overdue follow-ups,
evidence-confirmed deceased owners, pipeline distribution, call outcomes, and
the highest-priority active opportunities.

Recommendations are deterministic responses to recorded gaps such as overdue
follow-ups, missing contact data, and unresolved research. The report uses the
latest CRM record per property so repeated imports do not inflate totals, and
it explicitly avoids estimating revenue, property value, or close probability.

## Provider-neutral enrichment exchange

The Enrichment workspace creates controlled contact-research batches for active
leads with neither phone nor email. Daryl supplies the source, per-record price,
hard budget cap, and maximum record count before the prioritized request CSV is
created. No subscription or provider connection is assumed.

Completed CSV or Excel results can be imported into the same batch. New contact
data fills empty fields and writes source activity to the lead. A different
phone or email never overwrites trusted CRM data: it is preserved as a recorded
conflict for review. Batch status, estimated spend, update count, conflicts,
and records without results remain available in PostgreSQL.

## Import operations center

The Imports workspace provides one durable ledger for every county-list run.
It accepts XLSX, XLS, and CSV uploads, starts the existing evidence-safe
qualification workflow, and shows each job's tax year, status, progress,
Assessor checks, downloadable artifact, and resume action.

Jobs and artifacts remain backed by PostgreSQL, so a Render restart does not
erase processing state. A resume link reopens the dashboard workflow at the
saved stage. Imports can come from a manual upload or the Google Drive file
picker. Drive authorization is read-only and ephemeral: the selected file is
copied into the durable processing ledger, while OAuth tokens are never stored.

Enable the optional Drive picker with `GOOGLE_DRIVE_CLIENT_ID` and
`GOOGLE_DRIVE_API_KEY`. The Google Cloud project must enable the Google Picker
and Drive APIs and authorize the production origin. If those variables are not
set, manual upload remains fully operational.

## Daily execution system

The Today workspace requires one check-in per Tulsa business day. Daryl records
the day's primary focus plus call and research targets before working the due
follow-up queue. The system uses the `America/Chicago` timezone rather than the
server timezone.

Progress is calculated from call logs and active evidence records created that
day, so activity is never entered twice. The live view shows targets, completed
work, and follow-ups still due. Closing the day preserves an optional summary
and leaves the complete activity history attached to its leads.

## Operational alert center

The Alerts workspace creates persistent in-app updates for overdue follow-ups,
conflicting deceased-owner evidence, processing failures, county lists ready
for approval, and enrichment conflicts. Each condition uses a stable
fingerprint, so repeated polling never creates duplicate alerts.

Unread counts appear on the dashboard and the alert inbox refreshes every
minute. Daryl can open the exact affected workflow, mark individual alerts
read, or clear the unread inbox while retaining its history. These are in-app
alerts remain the source of truth. Optional email digests deliver each unread
alert at most once, without changing its read state. Configure `ALERT_EMAIL_TO`,
`ALERT_EMAIL_FROM`, `PUBLIC_BASE_URL`, `SMTP_HOST`, `SMTP_PORT`,
`SMTP_USERNAME`, `SMTP_PASSWORD`, and a long random `ALERT_DELIVERY_TOKEN`.
Schedule a trusted POST to `/api/alerts/deliver` with
`Authorization: Bearer <ALERT_DELIVERY_TOKEN>`; an empty queue sends no email.
Urgent alerts can also be delivered once by SMS through Twilio. SMS is disabled
by default and requires explicit opt-in with `ALERT_SMS_ENABLED=true`, plus
`ALERT_SMS_TO`, `ALERT_SMS_FROM`, `TWILIO_ACCOUNT_SID` and
`TWILIO_AUTH_TOKEN`. Schedule the same trusted bearer token against
`POST /api/alerts/deliver-sms`. Only unread urgent alerts are sent; successful
delivery is recorded independently from email and never marks the inbox read.

## CRM database

Local development uses SQLite by default and creates its database at:

```text
data/leadcleaner.db
```

To use a different local location, set:

```bash
export CRM_DATABASE=/persistent/path/leadcleaner.db
```

Production uses PostgreSQL automatically when `DATABASE_URL` is present:

```bash
export DATABASE_URL=postgresql://user:password@host/database
```

The application accepts Render's internal PostgreSQL URL directly, creates the
schema idempotently on startup, and verifies connections before reusing them.
Keep the database in the same Render region as the web service and store the
internal URL as a secret environment variable named `DATABASE_URL`.

Render runs `python migrate.py` before Gunicorn accepts traffic. Schema changes
use an idempotent migration, a PostgreSQL advisory lock, and strict lock and
statement timeouts. The contact ledger table is prepared independently from
its application interface, so a schema deploy can be validated before the
feature begins reading or writing contact data.

### Durable processing storage

Processing jobs no longer depend on Render's ephemeral filesystem. PostgreSQL
stores job metadata plus the source, qualification, Assessor-verified, and
skip-trace workbooks. The local `uploads/` and `outputs/` directories are only
a fast, rebuildable cache.

After a restart or deploy, the dashboard, downloads, Assessor batches, approval
preview, and explicit CRM import restore the required artifacts from the
database automatically. This deliberately favors workflow durability for the
current low-frequency list cadence; retention policies or external object
storage can be added when file volume makes that worthwhile.

Each processing run also exposes a persisted workflow state:

```text
Upload received → Qualifying records → Ready for Assessor
→ Assessor in progress → Ready for approval → Imported to CRM
```

The dashboard shows that state and can reopen any recent run. Assessor counts
are cumulative, so verification can continue safely across sessions, devices,
restarts, or deploys. Failed uploads are retained with an error state instead
of disappearing from the operational history.

### Private Render access

The production workspace is locked by default. Configure these Render
environment variables before deploying this version:

```text
APP_LOGIN_EMAIL=daryl@example.com
APP_LOGIN_PASSWORD=<a unique password shared privately with Daryl>
SECRET_KEY=<a long random value>
```

Generate `SECRET_KEY` locally with:

```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Never place those values in GitHub, screenshots, PRs, or application logs.
Authentication uses a 12-hour secure session cookie, CSRF validation on every
mutation, and rate limiting after repeated failed sign-in attempts. The health
endpoint remains public so Render can verify the service without credentials.

The health endpoint confirms which database engine is active without exposing
credentials:

```text
GET /api/health
{"database":"postgresql","status":"ok"}
```

Render uses `GET /api/readiness` for deployment readiness. It returns HTTP 200
only when PostgreSQL, private access, secrets, and every enabled optional
integration form a complete production configuration. The response contains
only missing or invalid variable names, never their values. Use `/api/health`
for liveness and `/api/readiness` for release/deployment checks.

Do not commit or paste the database URL into source code, issues, or chat.

### Campaign workspaces and selective skip trace

Every approved import creates an independent campaign. Dashboard, lead queue,
pipeline and skip-trace eligibility can be scoped to that campaign, while
contact intelligence is inherited when the same property returns in a later
list. Campaigns can be completed or archived without deleting audit history.

Selective skip tracing uses Tracerfy's synchronous owner-lookup API. Configure
`TRACERFY_API_TOKEN`, select a campaign, and choose high-priority or
potentially-deceased candidates. The UI shows a hard maximum before a second
execution confirmation. Results enter the Contact Ledger directly; DNC/TCPA
numbers are blocked and provider, actual cost, misses, and failures remain
auditable. The adapter is isolated in `skip_trace.py`, so a provider can be
replaced without changing the CRM domain model.

---

## To stop the app

Press `Ctrl + C` in the terminal.
