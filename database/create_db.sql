-- author = 'mart2010'
-- copyright = "Copyright 2016, The HRD Project"


-- As superuser, create role hrd
	create role hrd with login password 'hrd';
	alter role hrd CREATEROLE;
	create database hrd owner= hrd;

-- As superuser, switch to new db and revoke privileges to other users */
	\c hrd
	revoke connect on database hrd from public;
	revoke all on schema public from public;
	grant all on schema public to hrd;


-- used to backup database
pg_dump -f hrd_20160727.sql  --schema=staging --schema=integration -U hrd  -p 54355 hrd
