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
    print(repr(h))


def test_error_hub():

    def expect_exception(yaml_txt):
        s_o = yaml.load(yaml_txt)
        with pytest.raises(Exception):
            s_o.validate()
        return s_o

    no_nat_keys = """
        !Hub
          natt_keyys:
            n_id: {type: number, size: 9}   
        """
    expect_exception(no_nat_keys)

    sur_key_no_seq = """
        !Hub
          nat_keys:
            h_id: {type: number, size: 9}
          sur_key:
            h_key: {seqq: 'wrong name'} 
        """
    s_o = expect_exception(sur_key_no_seq)

    two_nat_keys_no_sur = """
        !Hub
          nat_keys:
            h_id1: {type: number, size: 9}
            h_id2: {type: number, size: 9}
        """
    t_o = expect_exception(two_nat_keys_no_sur)

    two_sur_keys = """
        !Hub
          nat_keys:
            h_id1: {type: number, size: 9}
          sur_key:
            h_key: {seqq: 'wrong name'} 
            h2_key: {wrong: 1}
        """
    t_s = expect_exception(two_sur_keys)

    no_error = """
        !Hub
          nat_keys:
            h_id1: {type: number, size: 9}
            h_id2: {type: number, size: 9}
          sur_key:
            h_key1: {type: number, size: 9, sequence: my_seq}
        """
    no_o = yaml.load(no_error)
    print(repr(no_o))
    no_o.validate()



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


