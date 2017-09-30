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
    # used in conjunction with defaults found in template which have precendence over next values
    default_default = { 'hub':      {'sur_key.name': "<name>_key", 'sur_key.format': "NUMBER(9)"},
                        'link':     {'sur_key.name': "<name>_key", 'for_keys.name': "<hubs.primary_key>"},
                        'sat':      {'for_key.name': "<hub.primary_key>", 'lfc_dts': "effective_date"},
                        'satlink':  {'for_key.name': "<link.sur_key>", 'lfc_dts': "effective_date"} }
    
    # ddl_template is a dict read off from yaml file (either inside model or separate file defined by user)
    def __init__(self, dv_model, ddl_template):
        self.dv_model = dv_model
        self.ddl_template = dict(ddl_template)
        self.defaults = dict(ddl_template.get('defaults'))
    
    def ddl_template(self, dv_obj):
        # custom template 
        if dv_obj.ddl_custom:
            template = self.ddl_template[dv_obj.ddl_custom]
        else:
            template = self.ddl_template[dv_obj.__class__.__name__]
        if template is None:
            raise DefinitionError(dv_obj, "DDL template '{0}' definition not found".format(template))
        return template
                          
    def ddl(self):
        for name, dv_obj in self.dv_model.tables:
            ddl_obj = DDLObject(dv_obj, self.ddl_template(dv_obj), self.defaults[dv_obj.__class__.__name__])
            yield ddl_obj.ddl()
        
        
class DDLObject(object):
    regex_bracket = re.compile(r'(<[^>]+>)') 
    # special "comma case" ('<***,>')  where text is combined on same line
    regex_comma = re.compile(r'(<[^>]+,>)')    
           
    def __init__(self, dv_obj, template, default_keywords):
        self.dv_obj
        self.template = template
        self.default_keywords = default_keywords
        
    def substitute_text(self, text):
        """Generate a number of new text where all <objs.prop> found are resolved. The number of text
        generated depends on the number of elements returned while resolving.
        """
        keywords = self.regex_bracket.findall(text)
        values_per_keyword = [ [v for v in resolve_with_default(self.dv_obj,kw,self.default_keywords)] for kw in keywords]
        
        for no_line in range(values_per_keyword[0]):
            txt_line = text
            for i, kw in enumerate(keywords):
                try:
                    value = values_per_keyword[i][no_line]
                except IndexError as er:
                    raise DefinitionError(self.dv_obj, "Incompatible number of values resolved by '{}'".format(keywords[i]))
                txt_line = txt_line.replace(kw, value)
            yield txt_line

    def ddl(self):
        ddl = self.template
        # 1st process "comma case" 
        for match in self.regex_comma.findall(self.template):
            #programming check
            match.index(',>')
            kw = match.replace(',>','>')
            ddl = ddl.replace(match, ", ".join(resolve_with_default(self.dv_obj, kw, self.default_keywords)))

        # 2nd process normal multi-line case 
        ddl_list = ddl.split("\n")
        for i, line in enumerate(ddl_list):
            if self.regex_bracket.search(line):
                ddl_list[i:i+1] = self.substitute_text(line)
        return "\n".join(ddl_list)
                    

def resolve_with_default(dv_obj, keyword, default_keywords):
    """ Return values resolved by dv_obj, and when None resolve using the default_keyword found
    Argument keyword is with bracket!!
    """
    keyword = keyword[keyword.index('<')+1:keyword.index('>')].strip()
    value = resolve(dv_obj, keyword)
    if value is None or (len(value) == 1 and value[0] is None):
        default_kw = default_keywords.get(keyword)
        if default_kw is None:
            raise DefinitionError(dv_obj, "No default found for '{}'".format(keyword))
        if DDLObject.regex_bracket.search(default_kw) is None:
            return default_kw
        value = resolve(dv_obj, default_kw[default_kw.index('<')+1:default_kw.index('>')].strip())
        if value is None:
            raise DefinitionError(dv_obj, "Default keyword '{}' not resolvable".format(default_kw.strip()))
    return value
    
                
def resolve(dv_obj, keyword):
    """Return values resolved by dv_obj using the keyword as an attribute.  Argumemt keyword without bracket.
    """
    kw = keyword.split(".")
    if  len(kw) > 2:
        raise DefinitionError(dv_obj, "Resolving attribute '{0}' more than 1 level not supported".format(keyword))
    # no '.' 
    elif len(kw) == 1:
        value = [getattr(dv_obj, keyword)]
    # one "."     
    else:
        parent = getattr(dv_obj, kw[0])
        if parent is None:
             return None
        elif isinstance(parent, list):
            value = [getattr(child, kw[1]) for child in parent]
        else:
            value = [getattr(parent, kw[1])]      
    return value


  
    
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
                