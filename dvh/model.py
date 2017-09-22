# coding: utf-8
import ruamel.yaml
import re
import abc

# there will be a few type of errors/warnings:  
    # - Yaml-related (syntaxic or technical issues causing yaml.load  model to fail ) --> catch e and raise "MyBusiness" from e  done from the model reader..
    # - Domain/Model rules violation (domain-specific rules) raised from function: validate_model() (ex. a Link requires at least 2 hubs, a hub requires one ore more nat_keys
    # - SourceMapping: invalid/missing attribute for generating DML... ...and raised from these

    # 4- Log some warning, when generating code (DDL, DML...) requires falling back to default value  

class BaseError(Exception):
    def __init__(self, obj, msg):
        self.obj = obj
        self.msg = msg

    def __str__(self):
        return "Error for object {0} --> \t{1}".format(self.obj, self.msg)

class ModelRuleError(BaseError): pass
    
class DefinitionError(BaseError): pass

class SourceMappingError(BaseError): pass
        

class DVModel(object):

    def init_tables(self):
        for k, v in self.tables.items():
            v.define_name(k)    
    
    def validate_model(self):
        error = False
        for o in self.tables.values():
            try:
                o.validate_model()
            except (ModelRuleError, DefinitionError) as err:
                print(err)
                error = True
        if error:
            raise ModelRuleError("Found YAML definition error")
                                                
    def tables_in_creation_order(self):
        for k, _ in sorted(self.tables.items(), key=lambda kv: kv[1].creation_order + kv[0]):
            # print("ddl order key:" + k)
            yield k

    def generate_ddl(self, with_drop=False):
        if with_drop:
            for n in reversed(self.tables_in_creation_order()):
                print("I will drop: " + n)
        for n in self.names_ddl_order():
            print("I will create DDL for: " + n)

        
class ModelBase(object):

    def define_name(self, name):
        self.name = name
    
    def validate_model(self):
        raise NotImplementedError        
                  
    # yaml.load() is not using __init__, so default attribute are generated dynamically
    def __getattr__(self, name):
        # otherwise yaml fails when accessing: data.__setstate__(state)
        if name[:2] != '__':
            default = None
            setattr(self, name, default)
            return default
        else:
            raise AttributeError("'{0}' object has no attribute '{1}'".format(self.__class__.__name__, name))

    def __repr__(self):
        atts = ", ".join(["{0}={1}".format(k, repr(v)) for k, v in self.__dict__.items()])
        return "{0}({1})".format(self.__class__.__name__, atts)

    def __str__(self):
        return self.__repr__()

        
class Hub(ModelBase):
    creation_order = '1'

    @property
    def primary_key(self):
        """Rules to determine Primary key (property allows referencing like: hub.primary_key"""
        if self.sur_key:
            return self.sur_key['name']
        else:
            return list(self.nat_keys.keys())[0]

    def validate_model(self):
        if not isinstance(self.nat_keys, list):
            raise ModelRuleError(self, "Hub must have a 'nat_keys' list of at least one natural key")
        if self.sur_key is None and len(self.nat_keys) > 1:
            raise ModelRuleError(self, "Hub without 'sur_key' can only have one 'nat_keys' item (its Primary key)")


class Link(ModelBase):
    creation_order = '2'
            
    def validate_model(self):
        if not isinstance(self.hubs, list) or len(self.hubs) < 2:
            raise ModelRuleError(self, "Link must have a 'hubs' list of at least two Hubs")
        else:
            for h in self.hubs:
                if not(isinstance(h, Hub)):
                    raise ModelRuleError(self, "Link can only refer to Hub type")
            if self.for_keys and len(self.for_keys) != len(self.hubs):
                raise ModelRuleError(self, "Link's 'for_keys' mismatch the number of 'hubs'")
                                                    

class Sat(ModelBase):
    creation_order = '3'
    
    def validate_model(self):
        if self.hub is None:
            raise ModelRuleError(self, "Satellite must refer to a single 'hub'")
#        if self.atts is None or len(self.atts) < 1:
#            raise ModelRuleError(self, "Satellite must have at least one attribute listed under 'atts'")
#        if self.lfc_dts is None:
#            raise ModelRuleError(self, "Satellite must have one 'lfc_dts' attribute for lifecycle date/timestamp management")

        
class SatLink(ModelBase):
    creation_order = '4'
    
    def validate_model(self):
        if self.link is None:
            raise ModelRuleError(self, "Sat-Link must refer to a single 'link'")
#        if self.atts is None or len(self.atts) < 1:
#            raise ModelRuleError(self, "Sat-Link must have at least one attribute listed under 'atts'")
#        if self.lfc_dts is None:
#            raise ModelRuleError(self, "Sat-Link must have one 'lfc_dts' attribute for lifecycle date/timestamp management")

yaml = ruamel.yaml.YAML()
yaml.register_class(DVModel)
yaml.register_class(Hub)
yaml.register_class(Sat)
yaml.register_class(Link)
yaml.register_class(SatLink)

# yaml_tag approach does work for default optional param
# https://stackoverflow.com/questions/7224033/default-constructor-parameters-in-pyyaml
# so revert to using own constructor impl

# Generators assume objects satisfy model-rule (validate_model() called prior and raised no Exceptions)

class DDLGenerator(object):
    
    # ddl_templates is a dict read off from yaml file (either inside model or separate file and easily customize by user)
    def __init__(self, dv_model, ddl_templates):
        self.dv_model = dv_model
        # standardize all keys to be case insensitive
        for key, value in ddl_templates:
            ddl_templates.setdefault(key.lower(), value) 
        self.ddl_templates = ddl_templates
    
    def ddl_template(self, dv_obj):
        # custom template 
        if dv_obj.ddl_template:
            template = self.ddl_templates.get(dv_obj.ddl_template.lower())
        else:
            template = self.ddl_templates.get(dv_obj.__class__.lower())
        if template is None:
            raise DefinitionError(dv_obj, "DDL template '{0}' definition not found".format(template))
        return template
    
        
    def validate_ddl(self):
        raise NotImplementedError
        
    def ddl(self):     
        
        raise NotImplementedError

def resolve_keyword(dv_obj, txt_bracket, mandatory=False):
    """Resolve txt '[obj.prop]_suffix' and return a list of string appended with any suffix found from txt_bracket"""
    def try_resolve(obj, attr, mandatory):
        # no try/except, dv objects don't raise AttributeError on missing attribute
        val = getattr(obj, attr)
        if val is None and mandatory:
            raise DefinitionError(obj, "Mandatory attribute '{}' not resolvable".format(attr))
        return val   
    open_idx = txt_bracket.index('[')
    close_idx = txt_bracket.index(']')
    keyword = txt_bracket[open_idx+1:close_idx].strip()
    suffix = ""
    if close_idx + 1 < len(txt_bracket) and txt_bracket[close_idx+1] != " ":
        suffix = txt_bracket[close_idx+1:].strip()
    kw = keyword.split(".")
    if  len(kw) > 2:
        raise DefinitionError(dv_obj, "resolving attribute '{0}' is supported at only 2 levels".format(keyword))
    # one "." 
    elif len(kw) == 2:
        parent = try_resolve(dv_obj, kw[0], mandatory)
        if parent is None:
             return None
        elif isinstance(parent, list):
            value = [try_resolve(child, kw[1], mandatory) for child in parent]
        else:
            value = try_resolve(parent, kw[1], mandatory)
    # no '.' 
    else:
        value = try_resolve(dv_obj, keyword, mandatory)
   
    if value is None:
        return None    
    elif isinstance(value, list):
        return [v + suffix for v in value]
    elif isinstance(value, str):
        return [value + suffix] 
    else:
        raise Exception("Unexpected programming error")
             

def process_ddl_line(line):
    
    def resolve_with_default(dv_obj, exp_with_default):
        
        args = exp_with_default.split(',')
        val =  resolve_keyword(dv_obj, args[0])
        if val is None:
            default = args[0]
        pass
        

        
class DDLObject(object):
    # capture one '(' plus any chars, followed by ',' plus any chars, followed by one or more ')'
    regex_with_default = re.compile(r'(\([^,]+,[^\)]+\)+)')
    
    regex_curly = re.compile(r'{([^}]+)}')
        
    def __init__(self, dv_obj, template):
        self.dv_obj
        self.template = template
    
    def keywords_found(self):
        regex_kw = re.compile("{(\w+_*)}")
        return regex_kw.findall(self.template)
    
    def ddl(self):
        ddl = None
        # 1st process default value --> (attr, default)
        for exp in self.regex_with_default.findall(self.template):
            pass
                # TODO.. to continue
        
    
class DMLGenerator(object):
    def __init__(self, obj):
        self.obj = obj
        
    def get_source(self):
        if self.obj.src is None:
            pass
            # raise SourceMappingError(self.obj, "'src' attribute required to generate DML".format())
        return self.obj.src
        
    def validate_sourcing(self):
        raise NotImplementedError
        
    def dml(self):
        raise NotImplementedError
    

class DMLGeneratorHub(DMLGenerator):
        
    def validate_sourcing(self):        
        self.get_source()
        #Hub validate_model() has enforced the presence of nat_keys
        for v in self.obj.nat_keys.values():
            if v.get('src') is None:
                pass
                # raise SourceMappingError(self.obj, "'src' attribute required for every 'nat_keys'")
        if self.obj.sur_key and self.obj.sur_key.get('src') is None:
            pass
            # raise SourceMappingError(self.obj, "'sur_key' also required 'src' attribute for mapping (ex. a sequence: seq.nextval())")
        # etc...

        
        

class DMLGeneratorLink(DMLGenerator):

    def validate_sourcing(self):
        if self.obj.sur_key.get('src') is None:
            raise SourceMappingError(self.obj, "'src' attribute required for 'sur_key' (ex. a sequence: seq.nextval())")
        # Using default sourcing from hub
        if self.obj.src is None:
            hub_src = set()
            for h in self.obj.hubs:
                hub_gen = DMLGeneratorHub(h)
                try:
                    hub_gen.validate_sourcing() 
                except SourceMappingError as e:
                    raise SourceMappingError(self.obj, "Link sourcing inherited from Hub requires valid Hub sourcing")
                hub_src.add(h.src)
            if len(hub_src) > 1:
                raise SourceMappingError(self.obj, "Link sourcing inherited from Hub, requires same source for all Hub")

            
class DMLGeneratorSat(DMLGenerator):
        
    def __init__(self, obj):
        super().__init__(self, obj)
        # todo...
                
            
class DMLGeneratorSatLink(DMLGenerator):
        
    def __init__(self, obj):
        super().__init__(self, obj)
        # todo...
                