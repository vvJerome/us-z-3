# What This System Does

## The Problem

North Carolina Secretary of State business filings list the names and addresses of companies and their registered agents — but not their email addresses. To reach these businesses, you need to find those emails yourself.

This system automates that process. Given a list of business filings, it attempts to find a working email address for each one and confirms it actually exists before including it in the output.

## What Goes In

A file of business records, one per line. Each record contains the business name, the registered agent or officer name, and the state.

## What Comes Out

A spreadsheet of confirmed email addresses. Each row includes the business name, the contact name, the email address, how confident the system is that it is correct, and how that email was found and verified.

Only email addresses that have been actively confirmed as deliverable are included.

## How It Works

The system runs in two back-to-back stages.

### Stage 1 — Discovery

The goal is to find the most likely domain and email address for each business.

First, the system tries DNS. It guesses the business's domain name from its business name — for example, "Smith Electrical LLC" might map to `smithelectrical.com` — and checks whether that domain actually has a mail server. If it does, it generates a ranked list of likely email addresses for the contact name: patterns like `john.smith@smithelectrical.com`, `jsmith@smithelectrical.com`, and so on.

If DNS doesn't find anything, the system falls back to a web search. It searches for the business name, contact name, and state together, looking for their website or any page that mentions their email. If the search finds a result, it extracts the domain and generates the same kind of candidate list.

Records where neither DNS nor web search finds anything are marked as discovery failures. They are not tried again unless the run is manually reset.

### Stage 2 — Validation

Each candidate email is tested against three verification backends, tried in order from cheapest to most expensive.

**Microsoft probe.** For email domains hosted by Microsoft (Office 365, Outlook, Hotmail), the system uses a free Microsoft API to check directly whether an address exists. This is fast and costs nothing, so it runs first whenever applicable.

**Direct SMTP probing.** The system connects directly to the mail server for the domain and issues the standard SMTP handshake used to send email. The server's response tells whether the address is valid, rejected, or whether the domain accepts all incoming email. Two independent probers run at the same time and their results are compared.

**Zuhal rescue.** If both direct probers say an email is invalid, a third service — Zuhal — is asked as a tiebreaker. Zuhal costs a small amount per call and is only used when the first two agree on rejection. If Zuhal also rejects the address, the system tries the next candidate email.

If all candidate emails for a record are exhausted without any confirmation, the record is marked as a validation failure.

## Ranking and Learning

The system does not try candidate emails in a random order. It keeps a running record of which email patterns tend to succeed for each type of mail host — `firstname.lastname` might win frequently for Google Workspace domains, while `info` might win more often for small business hosts. Over time, the most-likely-to-succeed pattern is tried first, which reduces the number of calls needed per record.

## Cost and Speed

Discovery costs $0.001 per record for the web search fallback. Validation is free for Microsoft records, free for SMTP probing, and $0.0005 per Zuhal call (only for the subset that fails both SMTP probers).

A typical run of 1,000 records completes in a few minutes and costs under $1.50 in API fees.

## Resumability

Runs can be interrupted and resumed. The system tracks how far it has processed and picks up where it left off when restarted with the same run name.

## Audit Trail

Every record has a full history stored in a local database: what was tried, what each backend returned, when, and why the final verdict was reached. The output CSV is a summary; the database contains the complete picture.
