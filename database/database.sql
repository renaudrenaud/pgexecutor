----------------------------------------------------------------
TOC
----------------------------------------------------------------

1 - Table creation
2 - Function and trigger for 7 days limitation

-----------------------------------------------------------------


1 - Table creation

-- DROP TABLE public.cron_process;

CREATE TABLE public.cron_process (
	id serial4 NOT NULL,
	i_process int4 NULL,
	s_platform varchar(100) NULL,
	d_start timestamp NULL,
	d_end timestamp NULL,
	s_subject varchar(200) NULL,
	s_message varchar(500) NULL,
	s_deadline varchar(10) NULL,
	s_doc varchar(200) NULL,
	CONSTRAINT cron_process_pkey PRIMARY KEY (id)
);

2 - Function and trigger for 7 days limitation
CREATE OR REPLACE FUNCTION fdw_delete_old_records() RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM public.cron_process
    WHERE d_start > NOW() + INTERVAL '7 days';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER fdw_after_insert_trigger_cron_process
AFTER INSERT ON public.cron_process
FOR EACH ROW
EXECUTE FUNCTION fdw_delete_old_records();


