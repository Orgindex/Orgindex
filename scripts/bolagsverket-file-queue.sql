CREATE TABLE IF NOT EXISTS public.orgindex_bolagsverket_file_queue (
 source_url text PRIMARY KEY,
 status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','running','completed','failed')),
 attempts integer NOT NULL DEFAULT 0,
 next_attempt_at timestamptz NOT NULL DEFAULT now(),
 lease_until timestamptz,
 updated_at timestamptz NOT NULL DEFAULT now(),
 completed_at timestamptz,
 imported_rows integer,
 last_error text
);
