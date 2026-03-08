# Security Best Practices Review

## Executive Summary

Static review of the Flask application found two high-priority issues and three additional medium/low-priority gaps.

The highest-risk findings are:

- A committed iFood client secret in source control.
- A stored DOM-XSS path in the squads administration UI.

I did not run the application or execute dynamic security tests; this report is based on source inspection.

## Critical / High

### SBP-001
- Rule ID: FLASK-CONFIG-001
- Severity: Critical
- Location: `configure_ifood.py:11-12`, `configure_ifood.py:81-88`, `configure_ifood.py:121-122`
- Evidence:
  - `CLIENT_ID = "..."`
  - `CLIENT_SECRET = "..."`
  - `db.update_org_ifood_config(... client_secret=CLIENT_SECRET ...)`
  - `IFoodAPI(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, ...)`
- Impact: Anyone with repository access can use the committed credential until it is rotated, which can compromise the iFood integration and any tenant data reachable through it.
- Fix: Remove the secret from source immediately, rotate the exposed credential with iFood, load it only from environment/secret storage, and scrub it from setup scripts.
- Mitigation: If rotation cannot happen immediately, revoke access to the current credential and restrict repository access while rotating.
- False positive notes: This assumes the committed secret is real or was real at some point. Even if it is now invalid, it must still be removed from history and rotated if it was ever used.

### SBP-002
- Rule ID: FLASK-XSS-001
- Severity: High
- Location: `app_routes/core_admin_routes.py:575-626`, `dashboard_output/squads.html:261-275`
- Evidence:
  - Server accepts raw squad fields: `name = data.get('name', '').strip()` and `description = data.get('description', '').strip()`
  - Server stores them directly: `INSERT INTO squads (name, description, created_by, org_id) VALUES (%s, %s, %s, %s)`
  - Client renders them with `innerHTML` without escaping:
    - `${s.name}`
    - `${s.description}`
    - `${m.name}`
    - `${r.name}`
- Impact: A malicious org admin can persist JavaScript in squad names/descriptions or related display fields and execute code in another admin's browser, allowing session hijacking, CSRF bypass, or creation of public share links.
- Fix: Stop rendering untrusted values with `innerHTML`; use `textContent`/DOM APIs or a central HTML-escape helper before interpolation. Review similar patterns in `dashboard_output/*.html`.
- Mitigation: Add a CSP as defense-in-depth, but do not treat CSP as the primary fix.
- False positive notes: This is only safe if every rendered field is guaranteed trusted. That is not true here because squad data is admin-supplied and restaurant/member names can originate outside the page code.

## Medium

### SBP-003
- Rule ID: FLASK-HOST-001
- Severity: Medium
- Location: `dashboardserver.py:85-86`, `dashboardserver.py:1221-1225`, `app_routes/org_routes.py:434-438`, `app_routes/core_pages_routes.py:431-432`, `app_routes/groups_routes.py:599-603`
- Evidence:
  - `ProxyFix(... x_host=1 ...)`
  - `return request.host_url.rstrip('/')`
  - Public URLs are built from that host value:
    - `invite_url = f"{get_public_base_url()}/invite/{token}"`
    - `share_url = f"{get_public_base_url()}/dashboard?shared_view={shared['token']}"`
    - `share_url = f"{get_public_base_url()}/grupo/share/{link['token']}"`
- Impact: Without host validation, a forged `Host`/`X-Forwarded-Host` header can make the app generate invite/share URLs pointing at an attacker-controlled domain, enabling phishing and token leakage.
- Fix: Set `TRUSTED_HOSTS` in production, prefer a fixed `PUBLIC_BASE_URL`, and avoid deriving external links from request headers unless the host is validated.
- Mitigation: Enforce host allowlists at the reverse proxy as well.
- False positive notes: If the reverse proxy already rejects unapproved hosts, runtime risk is reduced, but the protection is not visible in this repository.

### SBP-004
- Rule ID: FLASK-HTTP-001 / FLASK-CSRF-001
- Severity: Medium
- Location: `app_routes/core_pages_routes.py:95-109`, `dashboardserver.py:1280-1304`, `dashboarddb.py:1131-1155`
- Evidence:
  - `GET /invite/<token>` calls `db.accept_invite(...)`
  - The CSRF guard only applies to `POST/PUT/PATCH/DELETE` under `/api/`
  - `accept_invite` changes membership state when the logged-in user's email matches the invite
- Impact: Invite acceptance is a state-changing action reachable by `GET`, so it bypasses the app's CSRF control path and can be triggered by simple navigation if the victim is already logged in and a valid invite URL is known.
- Fix: Make invite acceptance `POST`-only, require the existing CSRF mechanism, and keep `/invite/<token>` as a landing page that asks the user to confirm via a protected API call.
- Mitigation: Reduce token lifetime and make invite acceptance idempotent with explicit confirmation.
- False positive notes: Email matching limits abuse to the intended recipient, but it does not remove the unsafe-method/CSRF design flaw.

## Low

### SBP-005
- Rule ID: FLASK-HEADERS-001 / FLASK-LIMITS-001
- Severity: Low
- Location: `dashboardserver.py:175-192`
- Evidence:
  - The global `after_request` hook only sets cache headers.
  - Repository search found no app-level configuration for `Content-Security-Policy`, `X-Frame-Options`, `X-Content-Type-Options`, `Referrer-Policy`, `TRUSTED_HOSTS`, or `MAX_CONTENT_LENGTH`.
- Impact: The app is missing common defense-in-depth controls against clickjacking, MIME sniffing, some XSS classes, host-header abuse, and oversized request bodies.
- Fix: Add central security headers, set `TRUSTED_HOSTS`, and define conservative request-size limits (`MAX_CONTENT_LENGTH`, and proxy-side limits).
- Mitigation: If these are enforced at the CDN/proxy, document that explicitly and add deployment checks.
- False positive notes: These controls may exist outside the app. They are not visible in code or deployment config here.

### SBP-006
- Rule ID: FLASK-CONFIG-001
- Severity: Low
- Location: `dashboardserver.py:5566-5571`, `dashboarddb.py:744-760`, `resetpass.py:30-32`, `resetpass.py:59-62`, `resetpass.py:89-119`, `migrate_passwords.py:68-70`, `migrate_passwords.py:82`, `migrate_passwords.py:92-93`
- Evidence:
  - Runtime bootstrap can create default users if `BOOTSTRAP_DEFAULT_USERS=true`
  - Helper scripts reset accounts to predictable passwords such as `admin123`, `user123`, `Admin123!`, and `User123!`
  - `resetpass.py` also embeds local database credentials
- Impact: These scripts create an operational footgun; if they are run in the wrong environment, accounts can be reset to known credentials.
- Fix: Remove predictable defaults, require operator-supplied random passwords or one-time reset tokens, and move DB connection settings to environment variables only.
- Mitigation: Restrict script execution in production environments and clearly mark them as unsafe recovery tools if they must remain.
- False positive notes: The main application disables default-user bootstrap unless explicitly enabled, so this is not a default runtime exposure.

## Residual Risk / Gaps

- I did not find automated security tests covering invite acceptance, share-link generation, or front-end escaping paths.
- Public share tokens are generated with `secrets.token_urlsafe`, which is good, but several URLs still place tokens in browser-visible locations; review referrer/logging exposure when hardening those flows.
