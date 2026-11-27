CREATE TABLE IF NOT EXISTS public.orgindex_bv_archive_imports (
    source_url text PRIMARY KEY REFERENCES public.orgindex_bolagsverket_file_queue(source_url),
    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    downloaded_bytes bigint NOT NULL CHECK (downloaded_bytes > 0),
    report_count integer NOT NULL,
    parsed_rows integer NOT NULL,
    inserted_rows integer NOT NULL,
    unmatched_rows integer NOT NULL,
    extracted_records jsonb NOT NULL,
    diagnostics jsonb NOT NULL,
    parser_version text NOT NULL,
    imported_at timestamptz NOT NULL DEFAULT now()
);
