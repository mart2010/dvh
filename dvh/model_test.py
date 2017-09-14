# coding: utf-8
import pytest
from dvh.model import *
import sys

@pytest.fixture
def simple_model():
    pass


def test_default_att():
    y_t = """
        !Hub
            nat_keys:
                h1_id: {type: number, size: 9}
                h2_id: {type: date} 
        """
    h = yaml.load(y_t)
    assert h.sur_key is None
    assert h.nat_keys['h1_id']['type'] == 'number'
    assert h.nat_keys['h1_id']['size'] == 9
    #print(repr(h))

def callfct(function_call, error_expected=None):
    if error_expected:
        with pytest.raises(Exception) as e:
            function_call()
            assert e.value.__class__ == error_expected
    else:
        function_call()
 
def tst_hub():
    no_nat_keys = """
        !Hub
            natt_keyyys:
                n_id: {type: number, size: 9}   
        """
    s_o = yaml.load(no_nat_keys)
    callfct(s_o.validate_model, ModelRuleError)
    
    two_nat_keys_no_sur = """
        !Hub
            nat_keys:
                h_id1: {type: number, size: 9}
                h_id2: {type: number, size: 9}
        """
    s_o = yaml.load(two_nat_keys_no_sur)
    callfct(s_o.validate_model, ModelRuleError)

    no_error = """
        !Hub
            nat_keys:
                h_id1: {type: number, size: 9}
                h_id2: {type: number, size: 9}
            sur_key: {name: h_key1, type: number, size: 9}
        """
    s_o = yaml.load(no_error)
    callfct(s_o.validate_model)

    
dv_2hubs_txt = \
"""!DVModel
       tables: 
            h1: &h1 !Hub
                nat_keys: 
                    h1_id: {type: number}
            h2: !Hub &h2
                nat_keys: 
                    h2_id: {type: number}                 
"""
 
def tst_link(): 
    l_one_hub = dv_2hubs_txt + """
            l1: !Link
                hubs: [*h1]
    """
    s_o = yaml.load(l_one_hub)
    callfct(s_o.validate_model, ModelRuleError)

    link_with_default_fkey = dv_2hubs_txt + """
            l1: !Link
                hubs: [*h1, *h2]
                sur_key: {name: my_sur}
    """
    s_o = yaml.load(link_with_default_fkey)
    callfct(s_o.validate_model, ModelRuleError)
    
    link_with_fkey_mismatch = dv_2hubs_txt + """
            l1: !Link
                hubs: [*h1, *h2]
                for_keys: [{name: fk_h1}]
                sur_key: {name: my_sur}
    """
    s_o = yaml.load(link_with_fkey_mismatch)
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
    s_no_atts  = dv_2hubs_txt + """
            s1: !Sat 
                hub: *h1 
    """
    s_o = yaml.load(s_no_atts)
    callfct(s_o.validate_model, ModelRuleError)

    s_no_hub = dv_2hubs_txt + """
            s1: !Sat
                atts:
                    att1: {type: number}
    """
    s_o = yaml.load(s_no_hub)
    callfct(s_o.validate_model)

#    no_error = dv_2hubs_txt + """
#            s1: !Sat 
#                hub: *h1 
#                atts:
#                    att1: {type: number}
#                lfc_dts: {name: valid_from, type: date}
#    """
#    expect_err(no_error, nb_expected=0)


def tst_hub_mapping():
    hub_partial_mapping = """
        !Hub
            nat_keys:
                h_id1: {type: number, size: 9, src: s_id1}
                h_id2: {type: number, size: 9}
            sur_key: {name: h_key1, type: number, size: 9}
            src: src_hub
        """
    expect_err(hub_partial_mapping, nb_expected=2, validate_mapping_only=True)
    
    hub_ok_mapping = """
        !Hub
            nat_keys:
                h_id1: {type: number, size: 9, src: s_id1}
                h_id2: {type: number, size: 9, src: s_id2}
            sur_key: {name: h_key1, type: number, size: 9, src: seq}
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
    
    l_hub_withfkeys_no_mapping = dv_2hubs_txt + """
            l1: !Link
                hubs: [*h1, *h2]
                for_keys: [{name: fk1}, {name: fk2}]
                sur_key: {name: my_sur, src: my_seq}
                src: {name: src_tbl, hubs: [h1_src, h2_src]}, 
    """
    

ddl_template = """

Default:
    hub:      CREATE TABLE {name}_h (
              {sur_key} NUMBER(9),
              {nat_keys} NOT NULL,
              load_dts DATE NOT NULL,
              last_seen_date DATE,
              process_id NUMBER(9),
              rec_src VARCHAR2(200),
              CONSTRAINT {name}_pk PRIMARY_KEY ({sur_key}),
              UNIQUE ({nat_keys})
              );
    
    link:     CREATE TABLE {name}_l (
              {sur_key} NUMBER(9),
              {for_keys} NUMBER(9) NOT NULL,
              load_dts NOT NULL,
              last_seen_date DATE,
              process_id NUMBER(9),
              rec_src VARCHAR2(200),
              CONSTRAINT {name}_pk PRIMARY_KEY ({sur_key}),
              UNIQUE {for_keys},
              CONSTRAINT {name}_{hubs.name}_fk FOREIGN KEY ({for_keys}) REFERENCE {hubs.name}_h 
              );
    
    sat:      CREATE TABLE {name}_s (
              {hub.sur_key} NUMBER(9),
              effective_date DATE NOT NULL,
              expiration_date DATE NOT NULL DEFAULT to_date('40000101','YYYYMMDD'),
              {atts},
              process_id NUMBER(9),
              update_process_id NUMBER(9),
              rec_src VARCHAR2(200),
              CONSTRAINT {name}_pk PRIMARY_KEY ({hub.sur_key}, effective_date),
              CONSTRAINT {name}_{hub.name}_fk FOREIGN KEY ({hub.sur_key}) REFERENCE {hub.name}_h           
              );
    
    satlink:  CREATE TABLE {name}_sl (
              {link.sur_key} NUMBER(9),
              effective_date DATE NOT NULL,
              expiration_date DATE NOT NULL DEFAULT to_date('40000101','YYYYMMDD'),
              {atts},
              process_id NUMBER(9),
              update_process_id NUMBER(9),
              rec_src VARCHAR2(200),
              CONSTRAINT {name}_pk PRIMARY_KEY ({link.sur_key}, effective_date),
              CONSTRAINT {name}_{link.name}_fk FOREIGN KEY ({hub.sur_key}) REFERENCE {hub.name}_h
              );
    
Sat_with_deletion:
                


"""

