-- Preserve the existing verified contacts when enabling the shared Neon contact source.
INSERT INTO public.orgindex_company_contacts (orgnr,phone,phone_uri,phone_source_name,phone_source_url,phone_verified_at,phone_origin,website_url,website_source_name,website_source_url,website_verified_at,website_origin)
VALUES ('5566733472','031-82 48 18','tel:+4631824818','företagets webbplats','https://www.svenskkylservice.se/','2026-11-28','verified_site','https://www.svenskkylservice.se/','företagets webbplats','https://www.svenskkylservice.se/','2026-11-28','verified_site'),
('5594860594',NULL,NULL,NULL,NULL,NULL,NULL,'https://www.napsthlm.com/','företagets webbplats','https://www.napsthlm.com/','2026-11-28','verified_site'),
('5560094178','08-738 60 00','tel:+4687386000','Electrolux officiella kontaktuppgifter','https://www.electroluxgroup.com/en/electrolux-group-headquarters-17256/','2026-11-28','verified_site','https://www.electroluxgroup.com/','Electrolux officiella kontaktuppgifter','https://www.electroluxgroup.com/en/electrolux-group-headquarters-17256/','2026-11-28','verified_site')
ON CONFLICT (orgnr) DO NOTHING;

