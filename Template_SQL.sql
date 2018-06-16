/* Template used to generate the DDL/DML.  By default, DDL/DML statement is selected  
 * based on the object type using special tag.  
 * Alternatively, any object in yaml can use a custom DDL/DML with: "--[DDL|DML]_Type-custom:"
 * where custom must match a stmt in this template.  
 *
 */


/************************************************************************************************
/*										DDL Section
/************************************************************************************************/
 
--DDL_Hub:
CREATE TABLE <name>_h (
<sur_key.name> <sur_key.format>,
<nat_keys.name> <nat_keys.format> NOT NULL,
<extras.name> <extras.format>,
load_dts DATE NOT NULL,
last_seen_date DATE,
process_id NUMBER(9),
rec_src VARCHAR2(200),
UNIQUE (<unique_key>),
CONSTRAINT <name>_pk PRIMARY_KEY (<primary_key>)
);

--DDL_Link:
CREATE TABLE <name>_l (
<sur_key.name> <sur_key.format>,
<for_keys.name> <for_keys.format> NOT NULL,
<extras.name> <extras.format>,
load_dts NOT NULL,
last_seen_date DATE,
process_id NUMBER(9),
rec_src VARCHAR2(200),
UNIQUE (<unique_key>),
CONSTRAINT <name>_<hubs.name>_fk FOREIGN KEY (<for_keys.name>) REFERENCE <hubs.name>_h,
CONSTRAINT <name>_pk PRIMARY_KEY (<sur_key.name>)
);
    
--DDL_Sat:
CREATE TABLE <name>_s (
<for_key.name> <for_key.format>,
<oth_key.name> <oth_key.format>,
<lfc.name> <lfc.format>,
<lfc.exp> <lfc.format> NOT NULL DEFAULT to_date('40000101','YYYYMMDD'),
<atts.name> <atts.format>,
load_dts NOT NULL,
process_id NUMBER(9),
update_process_id NUMBER(9),
rec_src VARCHAR2(200),
CONSTRAINT <name>_pk PRIMARY_KEY (<primary_key>),
CONSTRAINT <name>_<hub.name>_fk FOREIGN KEY (<for_key.name>) REFERENCE <hub.name>_h 
);

--DDL_Satlink:
CREATE TABLE <name>_sl (
<for_key.name>,                     -- format taken from Link's primary_key
<lfc.name> <lfc.format>,
<lfc.exp> <lfc.format> NOT NULL DEFAULT to_date('40000101','YYYYMMDD'),
<atts.name>  <atts.format>,
load_dts NOT NULL,
process_id NUMBER(9),
update_process_id NUMBER(9),
rec_src VARCHAR2(200),
CONSTRAINT <name>_pk PRIMARY_KEY (<primary_key>),
CONSTRAINT <name>_<link.name>_fk FOREIGN KEY (<for_key.name>) REFERENCE <link.name>_l 
);
    
--DDL_Sat_multi_version: (not longer needed, as Sat has optional <oth.key>)




/************************************************************************************************
/*										DML Section
/************************************************************************************************/

--DML_Hub(1):
merge into <name>_h t using 
    select <nat_keys.src>, <lfc.src>, <rec_src>
    from <src> s on (<keys_join>)
when matched then update set t.last_seen_date = s.<lfc.src>
when not matched then insert(<sur_key.name>, <nat_keys.name>, <lfc.name>, last_seen_date, process_id, rec_src)
values (<sur_key.seq>, <nat_keys.src>, <lfc_dts.src>, <lfc_dts.src>, -111111, <rec_src>)
;

--DML_Hub_withdupes(1):
merge into <name>_h t using 
    (select <nat_keys.src>, min(<lfc.src>) as <lfc.src>, min(<rec_src>) as <rec_src>
     from {src}
     group by <nat_keys.src>
    ) s on (<keys_join>)
when matched then update set t.last_seen_date = s.<lfc.src>
when not matched then insert(<sur_key.name>, <nat_keys.name>, <lfc.name>, last_seen_date, process_id, rec_src)
values (<sur_key.seq>, <nat_keys.src>, <lfc.src>, <lfc.src>, -111111, <rec_src>)
;

--DML_Link(1):
merge into <name>_l t using 
    select <hubs.nat_keys.src>, <lfc_dts.src>, <rec_src>
     from <src> s on (<keys_join>)
when matched then update set t.last_seen_date = s.<lfc_dts.src>
when not matched then insert(<sur_key.name>, <for_keys.name>, effective_date, last_seen_date, process_id, rec_src)
values (<sur_key.seq>, <keys_source>, {effective_date_source}, {effective_date_source}, -111111, xml_filename)
;


--DML_Sat(1)
insert into <name>_s (<primary_key>, <lfc.exp>, <atts.name>, rec_src, process_id)
    select s.* from  
        (select  {keys_source}, min({effective_date_source}), {expiration_date},
                {atts_source}, 'N', min(xml_filename), -111111
        from <src>
        group by {keys_source}) s
        left join {table_target} t on ({keys_join} and t.expiration_date = {expiration_date} )
        where 
            {key_null} 
            or t.deleted = 'Y' {atts_comp}


