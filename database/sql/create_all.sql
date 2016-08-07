-- author = 'mart2010'
-- copyright = "Copyright 2016, The BRD Project"


-------------------------------------- Schema creation -------------------------------------
create schema staging;
create schema integration;


------------------------------------------ Staging layer -----------------------------------------------
--------------------------------------------------------------------------------------------------------
-- Goals:   - Layer where raw data is bulk loaded straight from source, so remaining
--            integration steps done by DB-engine (ELT).
--
--------------------------------------------------------------------------------------------------------
create table staging.audit (
    audit_id serial primary key,
    batch_job text,
    step_name text,
    step_no int,
    status text,
    run_dts timestamp,
    elapse_sec int,
    rows_impacted int,
    output text
);


create table staging.iso_country (
    sort int,
    name text,
    formal_name text,
    type text,
    sub_type text,
    sovereign text,
    capital text,
    iso_currency_code text,
    iso_currency_text text,
    itu_telephone text,
    iso_alpha2 char(2),
    iso_alpha3 char(3),
    iso_num int,
    iana_tld text
);



------------------------------------------ Integration layer -------------------------------------------
--------------------------------------------------------------------------------------------------------
-- Two sub-layers:
--          - 1) Raw sub-layer: untransformed data from source without applying business rules
--          - 2) Business sub-layer: apply some transformation to help preparing for presentation layer
--                    2.1) de-duplication (same_as for work, user, review, etc...)
--                    2.2) any sort of standardization/harmonization...
--
--------------------------------------------------------------------------------------------------------


create table integration.site (
    site_id int primary key,
    logical_name text unique,
    status text,
    create_dts timestamp not null
);

create table integration.site_identifier (
    site_id int not null,
    hostname text not null,
    full_url text,
    valid_from timestamp not null,
    valid_to timestamp,
    create_dts timestamp,
    update_dts timestamp,
    primary key (site_id, valid_from),
    foreign key (site_id) references integration.site(id) on delete cascade
);




