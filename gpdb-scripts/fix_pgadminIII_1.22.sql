CREATE OR REPLACE FUNCTION public.pg_get_function_result (the_oid OID) RETURNS TEXT
AS
$$
BEGIN
   RETURN format_type(the_oid, NULL);
END;
$$ LANGUAGE plpgsql IMMUTABLE
RETURNS NULL ON NULL INPUT;
