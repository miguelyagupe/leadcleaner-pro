# LeadCleaner Pro — portfolio evidence

**Status:** V1 code and tests are present in this public repository. A deployment configuration is present, but a publicly accessible running instance and client outcome metrics were not verified in the 20 November 2026 audit.

## Problem and implementation

LeadCleaner Pro turns property lead lists into a reviewed acquisition workflow. It qualifies and separates records with explicit reasons, checks current ownership against the county assessor, holds uncertain evidence for research, and requires approval before importing verified candidates into a CRM. The application also tracks calls, follow-ups, property history, enrichment requests and operational reports. See the detailed [README](README.md).

The owner conceived and directed the project and developed it with AI assistance. The repository demonstrates the implementation; it does not, by itself, establish a particular number of client records processed, hours saved, deals closed or production adoption.

## Verifiable code evidence

- [Qualification rules](qualification.py) and [qualification tests](tests/test_qualification.py).
- [Assessor verification](assessor.py) and [assessor tests](tests/test_assessor.py).
- [CRM workflow](crm.py) and [CRM tests](tests/test_crm.py).
- [Deployment configuration](render.yaml) and [environment variable template](.env.example).
- The [latest default-branch test run](https://github.com/miguelyagupe/leadcleaner-pro/actions/runs/31616961044) concluded successfully on 12 August 2026.

For a hiring review, lead with one small, explainable decision rule and its test, or show a synthetic-data walkthrough. Describe client results only when the source numbers and permission to disclose them are confirmed.

## Sharing review

This repository already contains client-specific context and a first name in its documentation and interface. Review client permission and remove or mask identifying information from any portfolio screenshots, data files or case study before sharing them. The `.env.example` values are examples; never include actual credentials or live lead records.

No license file is present. Public visibility does not grant a reuse license.
