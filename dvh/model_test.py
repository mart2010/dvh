import pytest
from dvh.model import *
import sys

@pytest.fixture
def simple_model():
    h1 = Hub({'h1_id': {'type': 'number', 'size': '(6)'}, 'h1_id2': {'type': 'date'}})
    s11 = Sat({'att1': {'type': 'number', 'size': '(3,2)'}, 'att2': {'type': 'varchar', 'size': '(15)'}}, h1)
    s12 = Sat({'att1': {'type': 'date'}, 'att2': {'type': 'varchar', 'size': '(15)'}}, h1)

    h2 = Hub({'h2_id': {'type': 'number', 'size': '(9)'}})
    s21 = Sat({'attx': {'type': 'number', 'size': '(10)'}, 'atty': {'type': 'varchar', 'size': '(15)'}}, h2)

    l = Link([h1, h2])
    sl1 = SatLink({'attt': {'type': 'date'}}, l)

    return DVModel({'sl1': sl1, 's21': s21, 'h2': h2, 'h1': h1, 's11': s11, 's12': s12, 'l': l})


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


def expect_err_msg(yaml_txt, nb_expected, msgs_like=None, validate_mapping_only=False):
    s_o = yaml.load(yaml_txt)
    #print(repr(s_o))
    if validate_mapping_only:
        msgs = list(s_o.validate_mapping())
    else:
        msgs = list(s_o.validate())
    print("\nresulted msg are: " + str(msgs))
    assert len(msgs) == nb_expected
    if msgs_like:
        for i, msg_like in enumerate(msgs_like):
            assert msgs[i].find(msg_like) >= 0
    if s_o:
        s_o.init_tables()
    return s_o


def tst_hub():
    no_nat_keys = """
        !Hub
            natt_keyyys:
                n_id: {type: number, size: 9}   
        """
    expect_err_msg(no_nat_keys, 1, ["Hub must have"])

    two_nat_keys_no_sur = """
        !Hub
            nat_keys:
                h_id1: {type: number, size: 9}
                h_id2: {type: number, size: 9}
        """
    expect_err_msg(two_nat_keys_no_sur, 1, ["without 'sur_key'"])

    no_error = """
        !Hub
            nat_keys:
                h_id1: {type: number, size: 9}
                h_id2: {type: number, size: 9}
            sur_key: {name: h_key1, type: number, size: 9}
        """
    expect_err_msg(no_error, nb_expected=0)

    
def test_hub_mapping():
    hub_partial_mapping = """
        !Hub
            nat_keys:
                h_id1: {type: number, size: 9, src: s_id1}
                h_id2: {type: number, size: 9}
            sur_key: {name: h_key1, type: number, size: 9}
            src: src_hub
        """
    expect_err_msg(hub_partial_mapping, nb_expected=2, validate_mapping_only=True)
    
    hub_ok_mapping = """
        !Hub
            nat_keys:
                h_id1: {type: number, size: 9, src: s_id1}
                h_id2: {type: number, size: 9, src: s_id2}
            sur_key: {name: h_key1, type: number, size: 9, src: seq}
            src: src_hub
        """
    expect_err_msg(hub_ok_mapping, nb_expected=0, validate_mapping_only=True)


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
 
def test_link():
    
    l_one_hub = dv_2hubs_txt + """
            l1: !Link
                hubs: [*h1]
    """
    expect_err_msg(l_one_hub, 1, ["Link must refer"])

    link_with_default_fkey = dv_2hubs_txt + """
            l1: !Link
                hubs: [*h1, *h2]
                sur_key: {name: my_sur}
    """
    expect_err_msg(link_with_default_fkey, 0)
    
    link_with_fkey_mismatch = dv_2hubs_txt + """
            l1: !Link
                hubs: [*h1, *h2]
                for_keys: [{name: fk_h1}]
                sur_key: {name: my_sur}
    """
    expect_err_msg(link_with_fkey_mismatch, 1)
    
    link_with_fkey_ok = dv_2hubs_txt + """
            l1: !Link
                hubs: [*h1, *h2]
                for_keys: [{name: fk_h1}, {name: fk_h2}]
                sur_key: {name: my_sur}
    """
    expect_err_msg(link_with_fkey_ok, 0)
    
    
def tst_link_mapping():
    
    l_hub_no_mapping = dv_2hubs_txt + """
            l1: !Link
                hubs: [*h1, *h2]
                sur_key: {name: my_sur, src: my_seq}
    """
    expect_err_msg(l_hub_no_mapping, 0, validate_mapping_only=True)
    
    l_hub_withfkeys_no_mapping = dv_2hubs_txt + """
            l1: !Link
                hubs: [*h1, *h2]
                fkeys: [{name: fk1}, {name: fk2}]
                sur_key: {name: my_sur, src: my_seq}
                src: {name: src_tbl, hubs: [h1_src, h2_src]}, 
    """
    
    
    
    
def tst_sat():
    dv_txt = """
       !DVModel
       tables: 
            h1: &h1 !Hub
                nat_keys: 
                    h1_id: {type: number}
    """
    s_no_atts = dv_txt + """
            s1: !Sat 
                hub: *h1 
    """
    expect_err_msg(s_no_atts, 2, ["must have at least one attribute", "must have a 'lfc_dts'"])

    s_no_hub = dv_txt + """
            s1: !Sat
                atts:
                    att1: {type: number}
    """
    expect_err_msg(s_no_hub, 2, ["must refer to one 'hub'", "must have a 'lfc_dts'"])

    no_error = dv_txt + """
            s1: !Sat 
                hub: *h1 
                atts:
                    att1: {type: number}
                lfc_dts: {name: valid_from, type: date}
    """
    expect_err_msg(no_error, nb_expected=0)



