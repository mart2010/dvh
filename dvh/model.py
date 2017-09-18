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
    pass


class ModelRuleError(BaseError):     
    def __init__(self, obj, msg):
        self.obj = obj
        self.msg = msg
    
    def __str__(self):
        return "Model error for {0} '{1}' --> \t{2}".format(self.obj.__class__.__name__, self.obj.name, self.msg)

class DefinitionError(BaseError):
    def __str__(self):
        return "Definition error for {0} '{1}' --> \t{2}".format(self.obj.__class__.__name__, self.obj.name, self.msg)

class SourceMappingError(BaseError):
    def __str__(self):
        return "Mapping Definition error for {0} '{1}' --> \t{2}".format(self.obj.__class__.__name__, self.obj.name, self.msg)

        
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
    """Rules to determine Primary key (@property allows referencing like: hub.primary_key"""
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

        
class DDLObject(object):
    
    # create static constructor based on type of object...
    # todo
    
    def __init__(self, dv_obj, template):
        self.dv_obj
        self.template = template
    
    def keywords_found(self):
        regex_kw = re.compile("{(\w+_*)}")
        return regex_kw.findall(self.template)
        
    def resolve_keyword(self, keyword):
        def try_resolve(obj, attr):
            # don't need to try/except, objects do not raise AttributeError on missing attribute 
            value = getattr(obj, attr)
            if value is None:
                raise DefinitionError(self.dv_obj, "DDL attribute '{0}' in template {1} not resolvable".format(attr, self.template))
            return value                
        
        kw = keyword.split(".")
        if  len(kw) > 2:
            raise DefinitionError(self.dv_obj, "DDL template attribute '{0}' is not valid".format(keyword))
        elif len(kw) == 1:
            value = self.try_resolve(self.dv_obj, keyword)
        # one "."
        else:
            parent = self.try_resolve(self.dv_obj, kw[0])
            # to do check if containers...
            if type(parent) is list:
                value = [self.try_resolve(child, kw[1]) for child in parent]
            else:
                value = try_resolve(parent, kw[1])    
        return value
  

        
        
        
class DDLHub(DDLBase):
            
    def ddl(self):
        pass

        
class DDLLink(object):
    
    def __init__(self, obj):
        self.obj = obj
        # test various condition that must be satified for DDL geneeration
        # maybe model-rule check enough and nothing to add here?...
        
    def ddl():
        pass

        
class DDLSat(object):
    
    def __init__(self, obj):
        self.obj = obj
        # test various condition that must be satified for DDL geneeration
        # maybe model-rule check enough and nothing to add here?...
        
    def ddl():
        pass

        
class DDLSatLink(object):
    
    def __init__(self, obj):
        self.obj = obj
        # test various condition that must be satified for DDL geneeration
        # maybe model-rule check enough and nothing to add here?...
        
    def ddl():
        pass

    
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
                