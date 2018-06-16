# coding: utf-8
import ruamel.yaml
import re
import os
from itertools import zip_longest
import argparse


class BaseError(Exception):
    def __init__(self, obj, msg):
        self.obj = obj
        self.msg = msg

    def __str__(self):
        return "Error for object {0}:\t{1}".format(self.obj, self.msg)


#For Domain/Model rules violation raised from function: Table.validate_rules()
class ModelRuleError(BaseError):pass
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
    """Superclass for all table-type in a DV model. Subclass's roles are to enforce/validate model rules 
    and to provide attribute values for DDL & DML generation.   They go through different states:
        - State0: Instanciation from yaml doc (only attributes present in doc exist at this point) 
        - State1: Initialized and Business rules validated; init() method 
        - State2: All atts mandatory set and DDL constructed from template;  setup_DDL() 
        - ...
    """
    # default's defaults (for cases where no default is set in yaml model)
    defaults_default= { 'Hub':      {'sur_key.name': "<name>_key", 'sur_key.format': "number(9)"},
                        'Link':     {'sur_key.name': "<name>_key", 'sur_key.format': "number(9)", 
                                     'for_keys.name': "<hubs.primary_key>", 'for_keys.format': "<hubs.primary_key_format>"},
                        'Sat':      {'for_key.name': "<hub.primary_key>", 'for_key.format': "<hub.primary_key_format>", 
                                     'lfc_dts.name': 'effective_date', 'lfc_dts.format': 'DATE' },
                        'satlink':  "todo" }
                        
    rx_keyword = re.compile(r'(<[^>]+>)')
    # verify .. want any alphanumeric char or . as prefix/suffix
    rx_kw_presuffix = re.compile(r'([\w\.]*<[^>]+>[\w\.]*)')
    
    
    def init(self, name, yaml_defaults=None):
        """For subclass to init and validate their state according to DV business rules. 
        """
        self.name = name
        c_name = self.__class__.__name__
        if yaml_defaults:
            self.defaults = {k: yaml_defaults.get(k,v) for k,v in self.defaults_default[c_name].items()} 
        else:
            self.defaults = self.defaults_default[c_name]
        self.validate_rules()
        
    def validate_rules(self):
        raise NotImplementedError
            
    def setup_DDL(self, template):
        """ Setting up DDL by ensuring all needed atts are set
        """
        # Let subclass setup all needed atts
        self._setup_atts_for_DDL()
        
        ddl_list =  template.split("\n")
        resolved_list = []
        for i, line in enumerate(ddl_list):
            new_lines = self.resolve_ddl_line(line)
            if new_lines:
                resolved_list += new_lines
        self.DDL = "\n".join(resolved_list)
                    
    def _setup_atts_for_DDL(self):
        raise NotImplementedError
    
        
    def setup_DML(self, templates):
        """ Setting up DML by ensuring all needed atts are set
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
        
                            

    def resolve(self, txt, txt_default=None, scalar=True, mandatory=True, list_join=", "):
        """Transform txt (or txt_default if txt is None) after having resolved keyword
        from self (ex '<name>_key' --> self.name + "_key"). 
        If no keyword found in txt, simply return it (or txt_default).
        """
        val = txt if txt else txt_default
        if not val:
            raise Exception("Programming error both txt and txt_default are None")
        elif val.find('<') == -1:
            return val
        prefix = val[:val.index('<')]
        postfix = val[val.index('>')+1:]
        keyword = val[val.index('<')+1:val.index('>')].strip()
        
        result = self.resolve_keyword(keyword, scalar, mandatory, list_join)
        if scalar and result:
            return prefix + result + postfix
        elif not scalar and isinstance(result,list):
            return [prefix + r + postfix for r in result]
        # for now we simply return other type of result (ex. dic) as-is
        return result
        
    def resolve_keyword(self, keyword, scalar, mandatory, list_join=", "):
        """ Resolve keyword and returns result as string when scalar=True (using list_join if result is a list).
        otherwise returns as list (transform as list if result is a string).
        Raise exception when None is resolved and mandatory is True.
        """
        try:
            result = self._resolve_keyword(keyword)
        except (KeyError, AttributeError):
            if mandatory:
                raise DefinitionError(self, "Mandatory Keyword '{}' resolved to None".format(keyword))
            else:
                return None
        
        if scalar:
            if isinstance(result, list):
                result = list_join.join(result)
            elif not isinstance(result, str):
                raise Exception(self, "Keyword '{}' resolved to type '{}' (unsupported with scalar=True)".format(keyword, type(result)))
        else:
            if isinstance(result, str):
                result = [result]
        return result
        
        
    def _resolve_keyword(self, keyword):
        """Resolve keyword (¨<level1> or <level1.level2>) from self.
        Raise eiher KeyError or AttributeError when no value resolvable.
        """
        kw = keyword.split(".")

        if  len(kw) > 2:
            raise DefinitionError(self, "Keyword '{0}' has more than two levels to resolve".format(keyword))
        # no '.' 
        elif len(kw) == 1:
            result = getattr(self, keyword)
        # one "."
        else:
            parent = getattr(self, kw[0])
            if isinstance(parent, list):
                if isinstance(parent[0], dict):
                    assert all(isinstance(c, dict) for c in parent)
                    result = [child[kw[1]] for child in parent]
                else:
                    result = [getattr(child, kw[1]) for child in parent]
            elif isinstance(parent, dict):
                result = parent[kw[1]]
            else: 
                result = getattr(parent, kw[1])
        return result
        
            
    def __repr__(self):
        atts = ", ".join(["{0}={1}".format(k, repr(v)) for k, v in self.__dict__.items()])
        return "{0}({1})".format(self.__class__.__name__, atts)

    def __str__(self):
        return self.__repr__()

        
class Hub(Table):
    creation_order = '1'
              
    def validate_rules(self):
        if not isinstance( getattr(self, 'nat_keys', None), list):
            raise ModelRuleError(self, "Hub must have a 'nat_keys' list of at least one natural key")
        if getattr(self, 'sur_key', None) is None and len(self.nat_keys) > 1:
            raise ModelRuleError(self, "Hub without 'sur_key' must have only ONE 'nat_keys' (used as its PK")
            
    def _setup_atts_for_DDL(self):
        if getattr(self, 'sur_key', None) is not None:
            self.sur_key['name'] = self.resolve(self.sur_key.get('name'), txt_default=self.defaults['sur_key.name'])
            self.sur_key['format'] = self.resolve(self.sur_key.get('format'), txt_default=self.defaults['sur_key.format'])            
            self.primary_key = self.sur_key['name']
            self.primary_key_format = self.sur_key['format']
            self.unique_key = ", ".join([n['name'] for n in self.nat_keys])
        else:
            self.sur_key = None
            self.primary_key = self.nat_keys[0]['name']
            self.primary_key_format = self.nat_keys[0]['format']
            self.unique_key = None

    def _setup_atts_for_DML(self):        
        src = self.resolve("s.<nat_keys.src>", scalar=False, mandatory=True)
        tgt = self.resolve("t.<nat_keys.name>", scalar=False, mandatory=True)
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
        self.sur_key['name'] = self.resolve(self.sur_key.get('name'), txt_default=self.defaults['sur_key.name'])
        self.sur_key['format'] = self.resolve(self.sur_key.get('format'), txt_default=self.defaults['sur_key.format'])
        
        # there's a list of FK name in yaml
        if getattr(self, 'for_keys', None) is not None:
            # names explicitly listed (format derived from Hubs.PK)
            self.unique_key = ", ".join(self.for_keys)
            self.for_keys = [ dict(name=l, format=self.hubs[i].primary_key_format) for i, l in enumerate(self.for_keys)] 
        else:
            # NOT EXACTLY RIGHT, SINCE ONYL THE DEFAULT BEHAVIOR IS IMPLEMENTED HERE...!!
            self.for_keys = [ dict(name=h.primary_key, format=h.primary_key_format) for h in self.hubs]
            self.unique_key = ", ".join([h.primary_key for h in self.hubs])

        def _setup_atts_for_DML(self):
                        
            
            
             
class Sat(Table):
    creation_order = '3'

    def validate_rules(self):
        if getattr(self, 'hub', None) is None:
            raise ModelRuleError(self, "Satellite must refer to one 'hub'")
#        if self.atts is None or len(self.atts) < 1:
#            raise ModelRuleError(self, "Satellite must have at least one attribute listed under 'atts'")
#        if self.lfc_dts is None:
#            raise ModelRuleError(self, "Satellite must have one 'lfc_dts' attribute for lifecycle date/timestamp management")

    def _setup_atts_for_DDL(self):       
        if getattr(self, 'for_key', None) is None:
            # for_key is MANDATORY
            self.for_key = {}            
        self.for_key['name'] = self.resolve(self.for_key.get('name'), txt_default=self.defaults['for_key.name'])
        self.for_key['format'] = self.resolve(self.for_key.get('format'), txt_default=self.defaults['for_key.format'])

        # lfc is NOT mandatory without any keyword <>
        if getattr(self, 'lfc_dts', None) is not None:
            l_name = self.lfc_dts.get('name', self.defaults['lfc_dts.name'])
            l_fmt = self.lfc_dts.get('format', self.defaults['lfc_dts.format'])
            self.lfc_dts = {'name': l_name, 'format': l_fmt}
            self.primary_key =  "{}, {}".format(self.for_key['name'], self.lfc_dts['name'] )
        else:
            self.lfc_dts = None
            self.primary_key =  self.for_key['name']

    
# TODO by inhereting from Sat!
class SatLink(Table):
    creation_order = '4'
    defaults =  {"for_key.name": "<link.sur_key>", "lfc_dts": "effective_date" }
    
    def validate_rules(self):
        if self.link is None:
            raise ModelRuleError(self, "Sat-Link must refer to a single 'link'")



    


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
    
    
