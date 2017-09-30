# coding: utf-8
import pytest
from dvh.model import *
import ruamel.yaml
import sys

@pytest.fixture
def simple_model():
    pass

## ------------------------------------------------------------------------------------------- ##
##          Test yaml Loading of model eand model rules checks
## ------------------------------------------------------------------------------------------- ##

def test_default_att():
    y_t = """
        !Hub
            nat_keys:
                - {name: h1_id, format: number(9)}
                - {name: h2_id, format: date} 
        """
    h = yaml.load(y_t)
    assert h.sur_key is None
    assert h.nat_keys[0]['name'] == 'h1_id'
    assert h.nat_keys[0]['format'] == 'number(9)'
    assert h.nat_keys[1].get('name') == 'h2_id'
    assert h.nat_keys[1].get('size') is None
    #print(repr(h))
   
    
def callfct(function_call, error_expected=None):
    if error_expected:
        with pytest.raises(Exception) as e:
            function_call()
            #TODO: not sure next line is executed at all?????
            assert e.value.__class__ == error_expected
    else:
        function_call()
 
def test_hub():
    inval_nat_keys = """
        !Hub
            nat_keys:
                invalid_def: {format: number(9)}
        """
    s_o = yaml.load(inval_nat_keys)
    callfct(s_o.validate_model, ModelRuleError)
    
    two_nat_keys_no_sur = """
        !Hub
            nat_keys:
                - {name: h_id1, format: number(9)}
                - {name: h_id2, format: number(9)}
        """
    s_o = yaml.load(two_nat_keys_no_sur)
    assert isinstance(s_o.nat_keys, list) 
    callfct(s_o.validate_model, ModelRuleError)

    no_error = """
        !Hub
            nat_keys:
                - {name: h_id1, format: number(9)}
                - {name: h_id2, format: number(9)}
            sur_key: {name: h_key1, format: number(9)}
        """
    s_o = yaml.load(no_error)
    assert isinstance(s_o.sur_key, dict)
    callfct(s_o.validate_model)

    
dv_2hubs_txt = \
"""!DVModel
       tables: 
            h1: &h1 !Hub
                nat_keys:
                    - {name: h1_id, format: number}
            h2: !Hub &h2
                nat_keys: 
                    - {name: h2_id, format: number}                 
"""
 
def test_link(): 
    l_one_hub = dv_2hubs_txt + """
            l1: !Link
                hubs: [*h1]
    """
    s_o = yaml.load(l_one_hub)
    assert s_o.tables['l1'].hubs[0] is s_o.tables['h1'] 
    callfct(s_o.validate_model, ModelRuleError)

    link_with_default_fkey = dv_2hubs_txt + """
            l1: !Link
                hubs: [*h1, *h2]
                sur_key: {name: my_sur}
    """
    s_o = yaml.load(link_with_default_fkey)
    assert s_o.tables['l1'].hubs[0] is s_o.tables['h1'] 
    assert s_o.tables['l1'].hubs[1] is s_o.tables['h2'] 
    assert isinstance(s_o.tables['l1'].sur_key, dict)
    
    callfct(s_o.validate_model)
    
    link_with_fkey_mismatch = dv_2hubs_txt + """
            l1: !Link
                hubs: [*h1, *h2]
                for_keys: [{name: fk_h1}]
                sur_key: {name: my_sur}
    """
    s_o = yaml.load(link_with_fkey_mismatch)
    assert isinstance(s_o.tables['l1'].for_keys, list)
    callfct(s_o.validate_model, ModelRuleError)
    
    link_with_fkey_ok = dv_2hubs_txt + """
            l1: !Link
                hubs: [*h1, *h2]
                for_keys: [{name: fk_h1}, {name: fk_h2}]
                sur_key: {name: my_sur}
    """
    s_o = yaml.load(link_with_fkey_ok)
    callfct(s_o.validate_model)
 
def test_sat():
    s_no_hub  = dv_2hubs_txt + """
            s1: !Sat 
                hubbb: *h1 
    """
    s_o = yaml.load(s_no_hub)
    assert isinstance(s_o.tables['s1'], Sat)
    assert isinstance(s_o.tables['s1'].hubbb, Hub)
    callfct(s_o.validate_model, ModelRuleError)

    no_error = dv_2hubs_txt + """
            s1: !Sat 
                hub: *h1 
                atts:
                    - {name: att1, format: number}
                    - {name: att2, format: varchar2(10)}
                lfc_dts: {name: valid_from, format: date}
    """
    s_o = yaml.load(no_error)
    assert s_o.tables['s1'].atts[0]['name'] == 'att1' 
    callfct(s_o.validate_model)

    
## ------------------------------------------------------------------------------------------- ##
##          Test DDL generation
## ------------------------------------------------------------------------------------------- ##

ddl_template = """
defaults:
    hub:  {sur_key.name: "<name>_key", sur_key.format: NUMBER(9)}
    link: {sur_key.name: "<name>_key", for_keys.name: "<hubs.primary_key>" }
    sat:  {for_key.name: "<hub.primary_key>", lfc_dts: effective_date }
    satlink:  {for_key.name: "<link.sur_key>", lfc_dts: effective_date }
           
hub:          CREATE TABLE <name>_h (
              <sur_key.name> <sur_key.format>,
              <nat_keys.name> <nat_keys.format> NOT NULL,
              <extras.name> <extras.format>,
              load_dts DATE NOT NULL,
              last_seen_date DATE,
              process_id NUMBER(9),
              rec_src VARCHAR2(200),
              CONSTRAINT <name>_pk PRIMARY_KEY (<sur_key.name>),
              UNIQUE (<nat_keys.name,>));

hub_no_sur:   CREATE TABLE <name>_h (
              <nat_keys.name> <nat_keys.format>,
              <extras.name> <extras.format>,
              load_dts DATE NOT NULL,
              last_seen_date DATE,
              process_id NUMBER(9),
              rec_src VARCHAR2(200),
              CONSTRAINT <name>_pk PRIMARY_KEY (<nat_keys.name>));

link:         CREATE TABLE <name>_l (
              <sur_key.name> <sur_key.format>,
              <for_keys.name> NOT NULL,     -- format FIXED to Hub's primary_key
              <extras.name> <extras.format>,
              load_dts NOT NULL,
              last_seen_date DATE,
              process_id NUMBER(9),
              rec_src VARCHAR2(200),
              CONSTRAINT <name>_pk PRIMARY_KEY (<sur_key.name>),
              UNIQUE (<for_keys.name,>),
              CONSTRAINT <name>_<hubs.name>_fk FOREIGN KEY (<for_keys.name>) REFERENCE <hubs.name>_h );
    
sat:          CREATE TABLE <name>_s (
              <for_key.name>,                  -- format FIXED to Hub's primary_key
              <lfc_dts.name> DATE NOT NULL,    --format FIXED to be aligned with expiration
              expiration_date DATE NOT NULL DEFAULT to_date('40000101','YYYYMMDD'),
              <atts.name> <atts.format>,
              process_id NUMBER(9),
              update_process_id NUMBER(9),
              rec_src VARCHAR2(200),
              CONSTRAINT <name>_pk PRIMARY_KEY (<for_key.name>, <lfc_dts.name>),
              CONSTRAINT <name>_<hub.name>_fk FOREIGN KEY (<for_key.name>) REFERENCE <hub.name>_h );
    
satlink:      CREATE TABLE <name>_sl (
              <for_key.name>,                     -- format FIXED to Link's primary_key
              <lfc_dts.name> DATE NOT NULL,       -- format FIXED to be aligned with expiration
              expiration_date NOT NULL DEFAULT to_date('40000101','YYYYMMDD'),
              <atts.name>  <atts.format>,
              process_id NUMBER(9),
              update_process_id NUMBER(9),
              rec_src VARCHAR2(200),
              CONSTRAINT <name>_pk PRIMARY_KEY (<for_key.name>, <lfc_dts.name>),
              CONSTRAINT <name>_<link.name>_fk FOREIGN KEY (<for_key.name>) REFERENCE <link.name>_l );
    
Sat_multi_version:
    sat:      CREATE TABLE <name>_s (
              <hub.nat_keys>,
              <other_key.name> <other_key.format>, 
              effective_date DATE NOT NULL,
              expiration_date DATE NOT NULL DEFAULT to_date('40000101','YYYYMMDD'),
              <atts.name> <atts.format>,
              process_id NUMBER(9),
              update_process_id NUMBER(9),
              rec_src VARCHAR2(200),
              CONSTRAINT <name}_pk PRIMARY_KEY (<hub.nat_keys>, <other_key>, effective_date),
              CONSTRAINT <name>_<hub.name>_fk FOREIGN KEY (<hub.nat_keys>) REFERENCE <hub.name>_h );
    
    satlink:  TODO...CREATE TABLE <name>_sl (
              <link.sur_key> NUMBER(9),
              effective_date DATE NOT NULL,
              expiration_date DATE NOT NULL DEFAULT to_date('40000101','YYYYMMDD'),
              <atts>,
              process_id NUMBER(9),
              update_process_id NUMBER(9),
              rec_src VARCHAR2(200),
              CONSTRAINT <name>_pk PRIMARY_KEY (<link.sur_key>, effective_date),
              CONSTRAINT <name>_<link.name>_fk FOREIGN KEY (<hub.sur_key>) REFERENCE <hub.name>_h );

"""
template = yaml.load(ddl_template)


def test_resolve_keyword():
    
    hub1 = Hub()
    hub1.name = 'hub1'
    assert resolve_keyword(hub1, "((<name>")[0] == hub1.name 
    assert resolve_keyword(hub1, "*(<name> no_suffix ")[0] == hub1.name
    assert resolve_keyword(hub1, "<name>_suffix ", mandatory=True)[0] == hub1.name + "_suffix"                          
    assert resolve_keyword(hub1, " <dummy>") i
    s None
    with pytest.raises(Exception) as e:
        resolve_keyword(hub1, " <dummy>", mandatory=True)
    assert e.value.__class__ == DefinitionError
    with pytest.raises(Exception) as e:
        resolve_keyword(hub1, " <dummy>", mandatory=True)
    assert e.value.__class__ == DefinitionError
    
    hub2 = Hub()
    hub2.name = 'hub2'
    link = Link()
    link.hubs = [hub1, hub2]
    assert resolve_keyword(link, "<hubs.name>") == ['hub1','hub2']
    assert resolve_keyword(link, "<hubs.name>_s") == ['hub1_s','hub2_s']                           

#propose strag:
#- replace {name} as easy
#- find the couple {} followed by space followed by any char  --> regex = r"({\w+})\s+(\S+)"
#- resolve the att var if we find format attr then replace also the format (second element found (except if 'NOT'))  
#-     
    
    
def tst_hub_mapping():
    hub_partial_mapping = """
        !Hub
            nat_keys:
                h_id1: {format: number, size: 9, src: s_id1}
                h_id2: {format: number, size: 9}
            sur_key: {name: h_key1, format: number, size: 9}
            src: src_hub
        """
    expect_err(hub_partial_mapping, nb_expected=2, validate_mapping_only=True)
    
    hub_ok_mapping = """
        !Hub
            nat_keys:
                h_id1: {format: number, size: 9, src: s_id1}
                h_id2: {format: number, size: 9, src: s_id2}
            sur_key: {name: h_key1, format: number, size: 9, src: seq}
            src: src_hub
        """
    expect_err(hub_ok_mapping, nb_expected=0, validate_mapping_only=True)

   

    
def tst_link_mapping():
    
    l_hub_no_mapping = dv_2hubs_txt + """
            l1: !Link
                hubs: [*h1, *h2]
                sur_key: {name: my_sur, src: my_seq}
    """
    expect_err(l_hub_no_mapping, 0, validate_mapping_only=True)
    
    l_hub_withfkeys_mapping = dv_2hubs_txt + """
            l1: !Link
                hubs: [*h1, *h2]
                for_keys: [{name: fk1}, {name: fk2}]
                sur_key: {name: my_sur, src: my_seq}
                src: {name: src_tbl, hubs: [h1_src, h2_src]}, 
    """
    


