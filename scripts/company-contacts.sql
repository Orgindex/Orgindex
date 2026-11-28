-- Orgindex company contacts schema, version 1 (2026-11-28).
-- This migration never resets SCB or Bolagsverket import state.
BEGIN;

CREATE TABLE IF NOT EXISTS public.orgindex_company_contacts (
  orgnr text PRIMARY KEY REFERENCES public.orgindex_companies(orgnr),
  phone text,
  phone_uri text,
  phone_source_name text,
  phone_source_url text,
  phone_verified_at timestamptz,
  phone_origin text,
  phone_marketing_block_type integer,
  website_url text,
  website_source_name text,
  website_source_url text,
  website_verified_at timestamptz,
  website_origin text,
  scb_checked_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT company_contacts_orgnr_format CHECK (orgnr ~ '^[0-9]{2}[2-9][0-9]{7}$'),
  CONSTRAINT company_contacts_phone_format CHECK (
    phone IS NULL OR (char_length(phone) BETWEEN 3 AND 64 AND phone = btrim(phone))
  ),
  CONSTRAINT company_contacts_phone_uri_format CHECK (
    phone_uri IS NULL OR (char_length(phone_uri) <= 24 AND phone_uri ~ '^tel:\+?[0-9]{3,15}$')
  ),
  CONSTRAINT company_contacts_phone_provenance CHECK (
    phone IS NULL OR (
      phone_uri IS NOT NULL AND phone_source_name IS NOT NULL
      AND char_length(btrim(phone_source_name)) BETWEEN 1 AND 160
      AND phone_source_url IS NOT NULL AND phone_verified_at IS NOT NULL
      AND phone_origin IS NOT NULL AND phone_origin IN ('registry', 'verified_site')
    )
  ),
  CONSTRAINT company_contacts_phone_source_url CHECK (
    phone_source_url IS NULL OR (char_length(phone_source_url) <= 2048
      AND phone_source_url ~ '^https?://[^/@[:space:]]+([/?#][^[:space:]]*)?$')
  ),
  CONSTRAINT company_contacts_phone_origin CHECK (
    phone_origin IS NULL OR phone_origin IN ('registry', 'verified_site')
  ),
  CONSTRAINT company_contacts_website_format CHECK (
    website_url IS NULL OR (char_length(website_url) <= 2048
      AND website_url ~ '^https?://[^/@[:space:]]+([/?#][^[:space:]]*)?$')
  ),
  CONSTRAINT company_contacts_website_provenance CHECK (
    website_url IS NULL OR (
      website_source_name IS NOT NULL
      AND char_length(btrim(website_source_name)) BETWEEN 1 AND 160
      AND website_source_url IS NOT NULL AND website_verified_at IS NOT NULL
      AND website_origin IS NOT NULL AND website_origin IN ('registry', 'verified_site')
    )
  ),
  CONSTRAINT company_contacts_website_source_url CHECK (
    website_source_url IS NULL OR (char_length(website_source_url) <= 2048
      AND website_source_url ~ '^https?://[^/@[:space:]]+([/?#][^[:space:]]*)?$')
  ),
  CONSTRAINT company_contacts_website_origin CHECK (
    website_origin IS NULL OR website_origin IN ('registry', 'verified_site')
  )
);

CREATE TABLE IF NOT EXISTS public.orgindex_contact_import_state (
  id integer PRIMARY KEY CHECK (id = 1),
  cursor_id bigint NOT NULL DEFAULT 0 CHECK (cursor_id >= 0),
  done boolean NOT NULL DEFAULT false,
  pages bigint NOT NULL DEFAULT 0 CHECK (pages >= 0),
  scanned_rows bigint NOT NULL DEFAULT 0 CHECK (scanned_rows >= 0),
  matched_rows bigint NOT NULL DEFAULT 0 CHECK (matched_rows >= 0),
  phone_rows bigint NOT NULL DEFAULT 0 CHECK (phone_rows >= 0),
  last_error text CHECK (last_error IS NULL OR char_length(last_error) <= 200),
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO public.orgindex_contact_import_state (id)
VALUES (1) ON CONFLICT (id) DO NOTHING;

COMMENT ON TABLE public.orgindex_company_contacts IS
  'v1: sourced company contacts; a missing value is not evidence that no contact exists.';
COMMENT ON COLUMN public.orgindex_company_contacts.phone_marketing_block_type IS
  'SCB telefonSparrTyp: marketing opt-out metadata, not an absence-of-phone flag.';
COMMENT ON COLUMN public.orgindex_contact_import_state.phone_rows IS
  'Cumulative matched rows processed with a phone present after upsert; not a count of newly discovered or unique contacts.';

COMMIT;
