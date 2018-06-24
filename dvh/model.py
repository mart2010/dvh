# coding: utf-8
import ruamel.yaml
import re
import os
from itertools import zip_longest
import argparse


####################################################################################################################
# aspect model à conserver ici
####################################################################################################################


class BaseError(Exception):
    def __init__(self, obj, msg):
        self.obj = obj
        self.msg = msg

    def __str__(self):
        return "Error for object {0}: {1}".format(self.obj, self.msg)


#For Domain/Model rules violation raised from function: Table.validate_rules()
class ModelRuleError(BaseError): pass
#Yaml-related causing yaml.load to fail, will raise DefinitionError by model reader...
class DefinitionError(BaseError): pass


class DVModel(object):
    """Represent a complete DataVault model defined inside a YAML document
    """
    def init_model(self):
        error = False
        for table_name, table_obj in self.tables.items():
            df = None
            if getattr(self, 'defaults', None) is not None:
                df = self.defaults.get(table_obj.__class__.__name__)
            try:
                table_obj.init(table_name, yaml_defaults=df)
            except (ModelRuleError, DefinitionError) as err:
                print(err)
                error = True
        if error:
            raise ModelRuleError(self, "Fix all model error(s) found in YAML definition")
        self.tables_in_create_order = sorted(self.tables.values(), key=lambda v: v.creation_order + v.name)
        
        
    def setup(self, template_dic, sql_type="DDL"):
        assert sql_type in ('DDL','DML')
        for table_obj in self.tables_in_create_order:
            tmpl = template_dic[sql_type + "_" + table_obj.__class__.__name__]
            if getattr(table_obj, sql_type + "_custom", None) is not None:
                tmpl = template_dic[sql_type + "_" + table_obj.__class__.__name__ + "_" + getattr(table_obj, sql_type + "_custom")]
            if sql_type == "DDL":
                table_obj.setup_DDL(template=tmpl)
            elif sql_type == "DML":
                table_obj.setup_DML(templates=tmpl)
                  
    
    def generate_ddl_stmts(self, with_sequence=True):
        if with_sequence:
            for t in reversed(self.tables_in_creation_order):
                print("I will drop: " + t)
        for t in self.tables_in_create_order:
            yield t.DDL
    
    def generate_drop_stmts(self, as_proc=True):
        # TODO implement drop stmtm indivudal or inside a single proc. (iterating an array of table name and a single catch error)
        pass

    def generate_ddl_grants(self, to_schema):
        pass
    

class PRESModel(object):
    """Represent a presentation model defined inside a YAML document
    """
    # TODO: this could be used to define View DDL !!!
    pass
    
    
        
class Table(object):
    """Superclass for all table-type in a DV model. Subclass must enforce/validate model rules 
    and setup attributes for DDL & DML generation, while going through these states:
        - State0: Instanciation from yaml doc (only attributes defined in yaml exist at this point) 
        - State1: Initializion and model rules validation (init() method executed)
        - State2: Atts for DDL construct are ready (setup_DDL() executed)
        - State3: Atts for DML construct are ready (setup_DML() executed)
        - ...
    """
    # default's defaults (applied when no default found in yaml model)
    defaults_default = \
        { 'Hub':  {'sur_key': dict(name="<name>_key", format="number(9)", seq="<name>_seq")},
          'Link': {'sur_key': dict(name="<name>_key", format= "number(9)", seq="<name>_seq"),
                   'for_keys': dict(name="<hubs.primary_key.name>", src="<hubs.nat_key.src>")},
          'Sat':  {'for_key': dict(name="<hub.primary_key.name>", src="<hub.nat_key.src>"), 
                   'lfc': dict(name="effective_date", exp="expiration_date", format= "date")},
          'Satlink':  "todo" 
         }
                        
    rx_keyword = re.compile(r'(<[^>]+>)')
    # verify .. want any alphanumeric char or . as prefix/suffix
    rx_kw_presuffix = re.compile(r'([\w\.]*<[^>]+>[\w\.]*)')
    
    
    def init(self, name, yaml_defaults=None):
        """For subclass to init and validate their state according to DV business rules. 
        """
        self.name = name
        self.table_type = self.__class__.__name__
        if yaml_defaults:
            self.defaults = {k: yaml_defaults.get(k,v) for k,v in self.defaults_default[self.table_type].items()} 
        else:
            self.defaults = self.defaults_default[self.table_type]
        self.validate_rules()
        
    def validate_rules(self):
        raise NotImplementedError
            
    def setup_DDL(self, template):
        """ Setting up DDL prerequisite with mandatory/default atts
        """
        # Let subclass setup all needed atts
        self._setup_atts_for_DDL()
        
#        ddl_list =  template.split("\n")
#        resolved_list = []
#        for i, line in enumerate(ddl_list):
#            new_lines = self.resolve_ddl_line(line)
#            if new_lines:
#                resolved_list += new_lines
#        self.DDL = "\n".join(resolved_list)
                    
    def _setup_atts_for_DDL(self):
        raise NotImplementedError
    
    def setup_DML(self, templates):
        """ Setting up DML by prerequisite with mandatory/default atts
        TO MERGE WITH setup_DDL...
        """
        # setup for DDL atts is a prerequisite.. A VOIR??
        if getattr(self, "DDL", None) is None:
            self._setup_atts_for_DDL()
        
        # Let subclass setup all needed atts
        self._setup_atts_for_DML()
        
        # Fillout the DML text from template step
        self.DMLs = []
        for step in templates:
            step_list =  step.splitlines()
            new_lines = []
            for line in step_list:
                replaced = self.resolve_dml_line(line)
                new_lines.append(replaced)
            self.DMLs.append("\n".join(new_lines))
                
        
    def _setup_atts_for_DML(self):
        raise NotImplementedError

    
    def resolve_ddl_line(self, ddl_line):
        """
        Transform ddl_line where each keywords (including pre/sufix, 'prefix<objs.prop>suffix') is resolved from self.
        and return as list (or None if any keyword resolved to None) 
        """
        kw_items = self.rx_keyword.findall(ddl_line)
        if len(kw_items) == 0:
            return [ddl_line]

        values_per_item = [self.resolve(k, scalar=False, mandatory=False) for k in kw_items]
        # print('substitute kw {0} en= {1}'.format(str(kw_items), str(values_per_item)))
        
        max_nb_values = 0
        for values in values_per_item:
            if not values:
                return None
            if len(values) > max_nb_values:
                max_nb_values = len(values)

        new_lines = []
        for no_line in range(max_nb_values):
            new_line = ddl_line
            for no_item in range(len(values_per_item)):
                newvalue = values_per_item[no_item][no_line] if len(values_per_item[no_item]) > no_line else values_per_item[no_item][0]
                new_line = new_line.replace(kw_items[no_item], newvalue)
            new_lines.append(new_line)
        return new_lines    
    
    
    def resolve_dml_line(self, dml_line):
        """
        Transform dml_line by resolving each keywords and join results into a string using ", ".
        """
        kw_items = self.rx_kw_presuffix.findall(dml_line)
        #print("voici ce que he trouve " + str(kw_items))
        if len(kw_items) == 0:
            return dml_line

        values_per_item = [self.resolve(k, scalar=False, mandatory=False) for k in kw_items]
        print('substitute kw {0} en= {1}'.format(str(kw_items), str(values_per_item)))
        
        new_line = dml_line
        for no_item, values in enumerate(values_per_item):
            if values:
                new_line.replace(kw_items[no_item], ", ".join(values))
            else:
                # TODO handle the comma after None if present..
                # i_after = text_line.find(kw_items[i]) + len(kw_items[i])
                new_line.replace(kw_items[no_item], "")
        return new_line
        

    def fillout_att_dict(self, att_dict, default_dict):
        """Complete att_dict by setting any missing value to default resolution. PAS CERTAIN DOIT GARDER: When resolution is a list 
        of one element, set value as this element instead of the list"""
        for k, v in default_dict.items():
            if att_dict.get(k) is None:    
                def_value = self.resolve_text(txt=v, join_with=None)
                if len(def_value) == 1:
                    att_dict[k] = def_value[0]
                else:
                    att_dict[k] = def_value
                     

    def resolve_text(self, txt, join_with=None):
        """Extract keyword from txt and resolve it from self (ex '<name>_key' --> self.name + "_key").
        If no keyword found in txt, simply return it. Use join_with to join list element to return string.
        """
        if isinstance(txt,list) or txt.find('<') == -1:
            return txt
        idx_1 = txt.index('<')
        idx_2 = txt.index('>')
        prefix = txt[:idx_1]
        postfix = txt[idx_2+1:]
        keyword = txt[idx_1+1:idx_2].strip()
        
        res = self.resolve_keyword(keyword, mandatory=True)
        res_prepost = [prefix + r + postfix for r in res]
        
        if join_with:
            return join_with.join(res_prepost)
        else:
            return res_prepost
        
    def resolve_keyword(self, keyword, mandatory, alt_obj=None):
        """Resolve keyword from self and return result as a list. 
        If impossible to resolve return None or Raise error when mandatory=True"""
        try:
            if alt_obj:
                result = list(self._resolve_recursive(obj=alt_obj, keyword=keyword))
            else:
                result = list(self._resolve_recursive(obj=self, keyword=keyword))
        except (KeyError, AttributeError):
            if mandatory:
                raise DefinitionError(self, "Mandatory Keyword '{}' resolved to None".format(keyword))
            else:
                result = None
        return result

    def _resolve_recursive(self, obj, keyword):
        """Resolve keyword from object (obj.keyword1.keyword2..keywordn) to get value(s) of keywordn. 
        Raise KeyError or AttributeError when None is found at any level."""
        kw = keyword.split(".")
        # recursive STOP condition
        if len(kw) == 1:
            if isinstance(obj, dict):
                yield obj[keyword]
            elif isinstance(obj, list):
                for o in obj: yield o
            else:
                yield getattr(obj, keyword)
        else:
            child = getattr(obj, kw[0])
            remaining_kw = ".".join(kw[1:])
            # TODO: list of list does not work properly in that case... (ex. hubs.nat_key.src) as the list are exploded.. see how to fix
            if isinstance(child, list):
                for c in child: yield from self._resolve_recursive(c, remaining_kw)
            elif isinstance(child, dict) or hasattr(child, '_resolve_recursive'):
                yield from self._resolve_recursive(child, remaining_kw)
            else:
                raise Exception("Recusrive programming error: obj:'{}', child:'{}', kw:'{}'".format(obj, child, keyword))
                        
            
    def __repr__(self):
        atts = ", ".join(["{0}={1}".format(k, repr(v)) for k, v in self.__dict__.items()])
        return "{0}({1})".format(self.__class__.__name__, atts)

    def __str__(self):
        return self.__repr__()

        
class Hub(Table):
    creation_order = '1'
              
    def validate_rules(self):
        if not isinstance( getattr(self, 'nat_key', None), list):
            raise ModelRuleError(self, "Hub must have a 'nat_key' list of at least one natural key")
        if getattr(self, 'sur_key', None) is None and len(self.nat_key) > 1:
            raise ModelRuleError(self, "Hub without 'sur_key' must have only ONE 'nat_key' (used as its PK")
            
    def _setup_atts_for_DDL(self):
        if getattr(self, 'sur_key', None) is not None:
            self.fillout_att_dict(self.sur_key, self.defaults['sur_key'])
            self.primary_key = {'name': self.sur_key['name'], 'format': self.sur_key['format']}
            self.unique_key = ", ".join([n['name'] for n in self.nat_key])
        else:
            self.sur_key = None
            self.primary_key = {'name': self.nat_key[0]['name'], 'format': self.nat_key[0]['format']}
            self.unique_key = None

    def _setup_atts_for_DML(self):        
        src = self.resolve("s.<nat_key.src>", scalar=False, mandatory=True)
        tgt = self.resolve("t.<nat_key.name>", scalar=False, mandatory=True)
        self.keys_join = " and ".join( t[0] + " = " + t[1] for t in zip(src, tgt) )
        

            
class Link(Table):
    creation_order = '2'

    def validate_rules(self):
        if not isinstance(getattr(self, 'hubs', None), list) or len(self.hubs) < 2:
            raise ModelRuleError(self, "Link must refer to two or more Hubs")
        else:
            for h in self.hubs:
                if not(isinstance(h, Hub)):
                    raise ModelRuleError(self, "Link can only refer to Hub type")
            if getattr(self, 'for_keys', None) is not None and len(self.for_keys) != len(self.hubs):
                raise ModelRuleError(self, "Link's 'for_keys' mismatch the number of 'hubs'")

    def _setup_atts_for_DDL(self):
        if getattr(self, 'sur_key', None) is None:
            # sur_key is MANDATORY
            self.sur_key = {}
        self.fillout_att_dict(self.sur_key, self.defaults['sur_key'])
        
        # there's no list of FK name in yaml
        if getattr(self, 'for_keys', None) is None:
            self.for_keys = [{}] * len(self.hubs)
        
        
        d_fmts = self.resolve_keyword('hubs.primary_key.format', mandatory=True)
        # workaround as list of list not yet suported in my resolve_recursive()...
        d_srcs = []
        for h in self.hubs:
            d_srcs.append(self.resolve_keyword('nat_key.src', mandatory=True, alt_obj=h))
        d_names = self.resolve_keyword('hubs.primary_key.name', mandatory=True)
        def_dict = [dict(name=d_names[i], src=d_srcs[i], format=d_fmts[i]) for i in range(len(self.hubs)) ]  
        
        for i, k in enumerate(self.for_keys):
            self.fillout_att_dict(k, def_dict[i])
        self.unique_key = ", ".join([v['name'] for v in self.for_keys])

    def _setup_atts_for_DML(self):
        if getattr(self, 'nat_keys_src', None) is not None:
            hubs_natkey_src =  [ self.src + "." + n for h in self.nat_keys_src for n in h] 
        else:    
            hubs_natkey_src = [ self.src + "." + n['src'] for h in self.hubs for n in h.nat_key] 
        hubs_natkey_tgt = [ h.name + "." + n['name'] for h in self.hubs for n in h.nat_key] 
        self.nat_keys_join = " and ".join([t[0] + " = " + t[1] for t in zip(hubs_natkey_src, hubs_natkey_tgt)])
        
        for_keys_tgt = self.resolve("t.<for_keys.name>", scalar=False, mandatory=True)
        # resolve only goes 2-level deep...
        for_keys_src = ["s." + h.primary_key['name'] for h in self.hubs]
        self.keys_join = " and ".join([ t[0] + " = " + t[1] for t in zip(for_keys_tgt, for_keys_src)])
            
            
             
class Sat(Table):
    creation_order = '3'

    def validate_rules(self):
        if getattr(self, 'hub', None) is None:
            raise ModelRuleError(self, "Satellite must refer to one 'hub'")

    def _setup_atts_for_DDL(self):       
        if getattr(self, 'for_key', None) is None:
            # for_key is MANDATORY
            self.for_key = {}
        self.fillout_att_dict(self.for_key, self.defaults['for_key'])

        # lfc is not MANDATORY
        if getattr(self, 'lfc', None) is None:
            self.primary_key =  self.for_key['name']
        else:
            self.fillout_att_dict(self.lfc, self.defaults['lfc'])
            self.primary_key =  "{}, {}".format(self.for_key['name'], self.lfc['name'] )
            

    
# TODO by inhereting from Sat!
class SatLink(Table):
    creation_order = '4'
    defaults =  {"for_key.name": "<link.sur_key>", "lfc_dts": "effective_date" }
    
    def validate_rules(self):
        if self.link is None:
            raise ModelRuleError(self, "Sat-Link must refer to a single 'link'")



    
####################################################################################################################
# aspect logistic à mettre dans un autre module
####################################################################################################################


DV_MODEL = None
def init_dv_model(yaml_model_file):
    global DV_MODEL
    if not DV_MODEL:
        yaml = ruamel.yaml.YAML()
        yaml.register_class(DVModel)
        yaml.register_class(Hub)
        yaml.register_class(Sat)
        yaml.register_class(Link)
        yaml.register_class(SatLink)       
        
        with open(yaml_model_file) as yf:
            DV_MODEL = yaml.load(yf)
        DV_MODEL.init_model()
                
    
# dict with DDL and DML {'DDL_Hub': 'ddl', .., 'DML_Sat': ['step1', 'step2',..]) 
template_SQL = None
def get_template_SQL():
    global template_SQL
    if template_SQL:
        return template_SQL
    
    module_path = os.path.dirname(__file__)
    # Template located at the ROOT of Project
    template = os.path.join(module_path, os.pardir, "Template_SQL.sql")
    try:       
        with open(template) as tf:
            content = "".join(tf.readlines())
    except NotADirectoryError:
        raise Exception("Could not find Template SQL '{}'".format(template))

    content_dic = {}
    regex = re.compile(r'(--(DDL|DML)_[^:]+:)', re.MULTILINE)
    for exp, _ in regex.findall(content):
        ibegin = content.index(exp) + len(exp) + 1
        iend = content.index(';',ibegin) + 1
        if exp.find('DDL') != -1:
            key = exp[exp.index('--')+2:exp.index(':')]
            content_dic[key] = content[ibegin:iend]
        elif exp.find('DML') != -1:
            key = exp[exp.index('--')+2:exp.index('(')]
            content_dic.setdefault(key, []).append(content[ibegin:iend])
    return content_dic


def get_args():
    parser = argparse.ArgumentParser(description="Utility for Data Vault code generation")
    parser.add_argument("-y", "--yaml", help="YAML file defining model and configuration")
    parser.add_argument("output", help="Output type: refresh_ddl, refresh_dml or chrono")
    return parser.parse_args()
    
    
def main():
    # args = get_args()
    init_dv_model("./Ex_model.yaml")
    DV_MODEL.setup_DDL(get_template_SQL())
    print(list(DV_MODEL.generate_ddl()))
    


if __name__ == '__main__':
    main()
    
    
