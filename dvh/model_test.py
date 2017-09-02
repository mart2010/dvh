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


def tst_default_att():

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


def expect_err_msg(yaml_txt, msgs_like, nb_expected=1):
    s_o = yaml.load(yaml_txt)
    #print(repr(s_o))
    msgs = list(s_o.validate())
    print("resulted msg are: " + str(msgs))
    assert len(msgs) == nb_expected
    for i, msg_like in enumerate(msgs_like):
        assert msgs[i].find(msg_like) >= 0
    return s_o


def tst_hub():

    no_nat_keys = """
        !Hub
            natt_keyys:
                n_id: {type: number, size: 9}   
        """
    expect_err_msg(no_nat_keys, ["Hub must have"])

    sur_key_no_seq = """
        !Hub
            nat_keys:
                h_id: {type: number, size: 9}
            sur_key: {name: h_key, seqq: 'wrong name'} 
        """
    expect_err_msg(sur_key_no_seq, ["'sequence'"])

    two_nat_keys_no_sur = """
        !Hub
            nat_keys:
                h_id1: {type: number, size: 9}
                h_id2: {type: number, size: 9}
        """
    expect_err_msg(two_nat_keys_no_sur, ["without 'sur_key'"])

    no_error = """
        !Hub
            nat_keys:
                h_id1: {type: number, size: 9}
                h_id2: {type: number, size: 9}
            sur_key: {name: h_key1, type: number, size: 9, sequence: my_seq}
        """
    no_o = yaml.load(no_error)
    no_o.validate()

def tst_link():
    dv_txt = """
       !DVModel
       tables: 
            h1: &h1 !Hub
                nat_keys: 
                    h1_id: {type: number}
            h2: !Hub &h2
                nat_keys: 
                    h2_id: {type: number}                 
    """

    l_one_hub = dv_txt + """
            l1: !Link
                hubs: [*h1]
    """
    expect_err_msg(l_one_hub, ["Link must refer to", "Link must have one 'sur_key'"], 2)

    l_missing_seq  = dv_txt + """
            l1: !Link
                hubs: [*h1, *h2]
                sur_key: {name: h_key1, type: number, size: 9, seqqq: my_seq} 
    """
    expect_err_msg(l_missing_seq, ["must have a 'sequence'"])

    no_error = dv_txt + """
            l1: !Link
                hubs: [*h1, *h2]
                sur_key: {sequence: my_seq}     
    """
    no_er_ob = yaml.load(no_error)
    no_er_ob.validate()

def test_sat():
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
    expect_err_msg(s_no_atts, ["must have at least one attribute", "must have a 'lfc_dts'"], 2)

    s_no_hub = dv_txt + """
            s1: !Sat
                atts:
                    att1: {type: number}
    """
    expect_err_msg(s_no_hub, ["must refer to one 'hub'", "must have a 'lfc_dts'"], 2)

    no_error = dv_txt + """
            s1: !Sat 
                hub: *h1 
                atts:
                    att1: {type: number}
                lfc_dts: {name: valid_from, type: date}
    """
    no_ob = yaml.load(no_error)
    no_ob.validate()


def tst_roundtrip(simple_model):
    serialized = ""
    yaml.dump(simple_model, serialized)
    print("the simple_model dump is --> \n" + serialized)

    #l = yaml.load(d)
    #print ("the loaded model is --->\n" + repr(l))
    #expected_order = ['h1', 'h2', 's11', 's12', 's21', 'l', 'sl1']
    #assert simple_model.names_ddl_order == expected_order



def tst_roundtrip(simple_model):

    yaml_simple = \
"""
 !DVModel
tables:
  l: &id003 !Link
    hubs:
    - &id001 !Hub
      keys:
        h1_id2:
          type: date
        h1_id:
          type: number
          size: (6)
    - &id002 !Hub
      surrogate_key:
      keys:
        h2_id:
          type: number
          size: (9)
  s12: !Sat
    hub: *id001
    atts:
      att2:
        type: varchar
        size: (15)
      att1:
        type: date
  s11: !Sat
    hub: *id001
    atts:
      att2:
        type: varchar
        size: (15)
      att1:
        type: number
        size: (3,2)
  s21: !Sat
    hub: *id002
    atts:
      atty:
        type: varchar
        size: (15)
      attx:
        type: number
        size: (10)
  h1: *id001
  sl1: !SatLink
    link: *id003
    atts:
      attt:
        type: date
  h2: *id002
"""

    expected_model = yaml.load(yaml_simple)
    print("shoudl fail as well " + repr(expected_model))
    print("le hub1 est-> " + repr(expected_model.tables['h1']))

    # assert expected_model == simple_model


