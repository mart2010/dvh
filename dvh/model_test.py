# coding: utf-8
import pytest
from dvh.model import *
import ruamel.yaml
import sys

@pytest.fixture
def simple_model():
    pass

def callfct(function_call, error_expected=None):
    if error_expected:
        with pytest.raises(Exception) as e:
            function_call()
            #TODO: not sure next line is executed at all?????
            assert e.value.__class__ == error_expected
    else:
        function_call()

    
    
## ------------------------------------------------------------------------------------------- ##
##          Test yaml Loading of model and model rules checks
## ------------------------------------------------------------------------------------------- ##
yaml = ruamel.yaml.YAML()
yaml.register_class(DVModel)
yaml.register_class(Hub)
yaml.register_class(Sat)
yaml.register_class(Link)
yaml.register_class(SatLink)

def test_yaml_and_resolve_atts():
    y_t = """
    tables: 
        hub: &hub !Hub
            name:  myhub
            nat_keys:
                - {name: h1_id, format: number(9)}
                - {name: h2_id}
            sur_key: {}
            list_vals:
                - val1
                - val2
            val_dic: {name: vdic_name, fmt: vdic_fmt}
        sat: &sat !Sat
            hub:  *hub
            att1: att1value
        """
    m = yaml.load(y_t)
    h = m['tables']['hub']
    assert h.nat_keys[0]['name'] == 'h1_id'
    assert h.nat_keys[1].get('name') == 'h2_id'
    assert h.nat_keys[1].get('format') is None
    assert h.sur_key == {}
    assert getattr(h, 'missing_att', None) is None
    try:
        getattr(h, 'missing_att') 
    except AttributeError:
        assert True
    
    s = m['tables']['sat']
    assert s.resolve_keyword('missing', mandatory=False) is None
    assert s.resolve_keyword('att1', mandatory=True) == ["att1value"]
    
    assert s.resolve_keyword('hub.name', mandatory=True) == ["myhub"]
    assert s.resolve_keyword('hub.list_vals', mandatory=True) == [["val1","val2"]]
    assert s.resolve_keyword('hub.sur_key', mandatory=True) == [{}]

    assert s.resolve_keyword('hub.nat_keys.name', mandatory=True) == ["h1_id", "h2_id"]
    assert s.resolve_keyword('hub.missing_att', mandatory=False) is None
    try:
        s.resolve_keyword('missing_att', mandatory=True)
    except DefinitionError:
        assert True
    assert s.resolve_keyword('hub.val_dic.name', mandatory=True) == ["vdic_name"]

    
dvmodel_txt = """
!DVModel
       defaults:
            Hub: {sur_key.format: "DUMMY_F"}
       tables: 
            h_surkey_nats: !Hub &h_surkey_nats 
                sur_key: {}
                nat_key:
                    - {name: h11_id, format: number(3), src: h1_src}
                    - {name: h12_id, format: char(10), src: h12_src} 
                    
            h_no_surkey_one_nat: &h_no_surkey_one_nat !Hub
                nat_key:
                    - {name: h2_id, format: number(9), src: h2_src}

            l_no: !Link
                hubs: [*h_surkey_nats, *h_no_surkey_one_nat]
                
            l_surkey: !Link
                hubs: [*h_surkey_nats, *h_no_surkey_one_nat]
                sur_key: {name: my_sur}
                
            l_forkey: !Link
                hubs: [*h_surkey_nats, *h_no_surkey_one_nat]
                for_keys: 
                    - {name: f1} 
                    - {name: f2}
                
            s_noforkey: !Sat
                hub:  *h_surkey_nats
                atts:
                    - {name: att1, format: number}
                    - {name: att2, format: varchar2(10)}
                lfc: {name: valid_from, format: datetime, src: lfc_src}
                
            s_forkey: !Sat
                hub:  *h_surkey_nats
                atts:
                    - {name: att1, format: int}
                for_key: {'name': 'fk_name'}
"""
dvmodel = yaml.load(dvmodel_txt)
dvmodel.init_model()

def test_ddl_setup():
    dvmodel.setup(get_template_SQL(), sql_type="DDL")
    
    with_sur = dvmodel.tables['h_surkey_nats']
    assert with_sur.name == 'h_surkey_nats'
    assert with_sur.sur_key['name'] == 'h_surkey_nats_key'
    assert with_sur.sur_key['format'] == with_sur.defaults['sur_key']['format']
    assert with_sur.primary_key['name'] == 'h_surkey_nats_key'
    assert with_sur.unique_key == 'h11_id, h12_id'
    
    no_sur = dvmodel.tables['h_no_surkey_one_nat']
    assert getattr(no_sur, 'sur_key', None) is None
    assert no_sur.primary_key['name'] == 'h2_id'
    assert getattr(no_sur, 'unique_key', None) is None
    
    l_no = dvmodel.tables['l_no']
    assert l_no.sur_key['name'] == 'l_no_key'
    
    l_surkey = dvmodel.tables['l_surkey']
    assert l_surkey.sur_key['name'] == "my_sur"
    assert l_surkey.for_keys[0] == {'name':with_sur.primary_key['name'], 'format':with_sur.primary_key['format'], 'src':['h1_src','h12_src']}
    assert l_surkey.for_keys[1] == {'name':no_sur.primary_key['name'], 'format':no_sur.primary_key['format'], 'src': ['h2_src']}
    
    l_forkey = dvmodel.tables['l_forkey']
    assert l_forkey.for_keys[0] == {'name':"f1", 'format':with_sur.primary_key_format} 
    assert l_forkey.for_keys[1] == {'name':"f2", 'format':no_sur.primary_key_format} 
    
    s_noforkey = dvmodel.tables['s_noforkey']
    assert s_noforkey.for_key == {'name': with_sur.primary_key, 'format': with_sur.primary_key_format}
    assert s_noforkey.lfc == {'name': 'valid_from', 'format': 'datetime', 'src': 'lfc_src'}
    assert s_noforkey.primary_key['name'] ==  s_noforkey.for_key['name'] + ", " + s_noforkey.lfc['name']
                                  
    s_forkey = dvmodel.tables['s_forkey']
    assert s_forkey.for_key == {'name': 'fk_name', 'format': s_forkey.hub.primary_key_format}
    assert getattr(s_forkey, 'lfc', None) is None
    assert s_forkey.primary_key['name'] ==  s_forkey.for_key['name']
    assert s_forkey.atts[0]['name'] ==  'att1'
    assert s_forkey.atts[0]['format'] ==  'int'

    
    with_sur_ddl = with_sur.DDL.splitlines()
    assert with_sur_ddl[0] == "CREATE TABLE h_surkey_nats_h ("
    assert with_sur_ddl[1] == "h_surkey_nats_key DUMMY_F,"
    assert with_sur_ddl[2] == "h11_id number(3) NOT NULL,"
    assert with_sur_ddl[3] == "h12_id char(10) NOT NULL,"
    assert with_sur_ddl[4] == "load_dts DATE NOT NULL,"
    assert with_sur_ddl[5] == "last_seen_date DATE,"
    assert with_sur_ddl[6] == "process_id NUMBER(9),"
    assert with_sur_ddl[7] == "rec_src VARCHAR2(200),"
    assert with_sur_ddl[8] == "UNIQUE (h11_id, h12_id),"
    assert with_sur_ddl[9] == "CONSTRAINT h_surkey_nats_pk PRIMARY_KEY (h_surkey_nats_key)"
    assert with_sur_ddl[10] == ");"

        
def test_dml_setup():    
    dvmodel.setup(get_template_SQL(), sql_type="DML")
    
    pass
    

    

def test_validate_rules():
# Still TORUN....
    # ------------------ Hub ------------------
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

    # ------------------ Link ------------------
    
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
                for_keys: [fk_h1]
                sur_key: {name: my_sur}
    """
    s_o = yaml.load(link_with_fkey_mismatch)
    assert isinstance(s_o.tables['l1'].for_keys, list)
    callfct(s_o.validate_model, ModelRuleError)
    
    link_with_fkey_ok = dv_2hubs_txt + """
            l1: !Link
                hubs: [*h1, *h2]
                for_keys: [fk_h1, fk_h2]
                sur_key: {name: my_sur}
    """
    s_o = yaml.load(link_with_fkey_ok)
    callfct(s_o.validate_model)
 
    # ------------------ Sat ------------------

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


    
    

