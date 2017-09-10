import ruamel.yaml


# there will be a few type of errors/warnings:  
    # - Yaml-related (syntaxic or technical issues causing yaml.load  model to fail ) --> catch e and raise "MyBusiness" from e  done from the model reader..
    # - Domain/Model rules violation (domain-specific rules) raised from function: validate_model() (ex. a Link requires at least 2 hubs, a hub requires one ore more nat_keys
    # - SourceMapping: invalid/missing attribute for generating DML... ...and raised from these

    # 4- Log some warning, when generating code (DDL, DML...) requires falling back to default value  

class BaseError(Exception):
    pass


class ModelRuleError(BaseException):     
    def __init__(self, obj, msg):
        self.obj = obj
        self.msg = msg
    
    def __str__(self):
        return "Model error for {0} '{1}' --> \t{2}".format(self.obj.__class__.__name__, self.obj.name, self.msg)

        
class SourceMappingError(ModelRuleError):
    def __str__(self):
        return "Source Mapping error for {0} '{1}' --> \t{2}".format(self.obj.__class__.__name__, self.obj.name, self.msg)
    
        

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

    def __ne__(self, other):
        return not self.__eq__(self, other)

    def __str__(self):
        return self.__repr__()


class DVModel(ModelBase):

    def init_tables(self):
        for k, v in self.tables.items():
            v.define_name(k)    
    
    def validate_model(self):
        error = False
        for o in self.tables.values():
            try:
                o.validate_model()
            except ModelRuleError as err:
                print(err)
                error = True
        if error:
            raise ModelRuleError("All model-rule error before")
                                                
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

    # def __repr__(self):
    #     return '%s(tables=%r)' % (self.__class__.__name__, self.tables)

    def __eq__(self, other):
        if not isinstance(other, DVModel):
            return False
        return self.tables == other.tables

# how to handle default values (i.e. when some att(fct returns None based on yaml):
    # the idea would be to decorate the function like primary_key(), unique_keys().. 
    # whose role will be to replace None with a default values
    # and these defaults would be either obtained in yaml or here in the code when nothing found in yaml
    # so for now, let's assume that None returned from model is OK

        
class Hub(ModelBase):
    creation_order = '1'
    # une idée afin de pouvoir mettre comme test (ds le Parent) que tous les attrs sont acceptés
    possible_atts = dict(nat_key="Key(s) used for identifying enitiy Hub in source, can replace 'sur_key' when unique", \
                         sur_key= "Auto generated key acting as Primary key (sequence or auto-increment)", \
                         src="Source to ....")
                
    def primary_key(self):
        if self.sur_key:
            return self.sur_key['name']
        else:
            return list(self.nat_keys.keys())[0]

    def unique_keys(self):
        if self.sur_key:
            return list(self.nat_keys.keys())
        else:
            return None  

    def validate_model(self):
        if self.nat_keys is None or len(self.nat_keys) == 0:
            raise ModelRuleError(self, "Hub must have at least one 'nat_keys'")
        if self.sur_key is None and len(self.nat_keys) > 1:
            raise ModelRuleError(self, "Hub without 'sur_key' can only have one 'nat_keys' which becomes Primary key")


    def __eq__(self, other):
        if not isinstance(other, Hub):
            return False
        return  self.name == other.name and self.keys == other.keys and self.surrogate_key == other.surrogate_key


class Link(ModelBase):
    creation_order = '2'

    def surrogate_key(self):
        if self.sur_key:
            return self.sur_key['name']
        else:
        # default (plus tard, se baser sur un objet "top-default" (mis dans le yaml DVModel) si défini, sinon retour val suivante:)
            return self.name + "_key"
    
    def foreign_keys(self):
        f_ks = []
        if self.for_keys:
            for k_d in self.for_keys:
                f_ks.append(k_d['name'])
        else:
            for h in self.hubs:
                # plus tard se baser sur un top-défaut si défini (qui par défaut pourrait mettre cette valeur)
                f_ks.append(h.primary_key())
        return f_ks
            
    def validate_model(self):
        if self.hubs is None or len(self.hubs) < 2:
            raise ModelRuleError(self, "Link must refer to more than one Hub")
        else:
            for h in self.hubs:
                if not(isinstance(h, Hub)):
                    raise ModelRuleError(self, "Link must refer to Hub type only")
            if self.for_keys and len(self.for_keys) != len(self.hubs):
                raise ModelRuleError(self, "Link's 'for_keys' mismatch the number of 'hubs'")
                                                    

    def __eq__(self, other):
        if not isinstance(other, Link):
            return False
        return self.name == other.name and self.hubs == other.hubs


class Sat(ModelBase):
    creation_order = '3'

    def validate_model(self):
        if self.hub is None:
            raise ModelRuleError(self, "Satellite must refer to one 'hub'")
        if self.atts is None or len(self.atts) < 1:
            raise ModelRuleError(self, "Satellite must have at least one attribute listed under 'atts'")
        if self.lfc_dts is None:
            raise ModelRuleError(self, "Satellite must have one 'lfc_dts' attribute for lifecycle date/timestamp management")

    def __eq__(self, other):
        if not isinstance(other, Sat):
            return False
        return self.name == other.name and self.atts == other.atts and self.hub == other.hub

        
class SatLink(ModelBase):
    creation_order = '4'

    def validate_model(self):
        if self.link is None:
            raise ModelRuleError(self, "Sat-Link must refer to one 'link'")
        if self.atts is None or len(self.atts) < 1:
            raise ModelRuleError(self, "Sat-Link must have at least one attribute listed under 'atts'")
        if self.lfc_dts is None:
            raise ModelRuleError(self, "Sat-Link must have one 'lfc_dts' attribute for lifecycle date/timestamp management")

    def __eq__(self, other):
        if not isinstance(other, SatLink):
            return False
        return self.atts == other.atts and self.link == other.link


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
    
    def __init__(self, obj):
        self.obj = obj
        
    def ddl():
        raise NotImplementedError
        
        
class DDLGeneratorHub(object):
    
    def __init__(self, obj):
        self.obj = obj
        # test various condition that must be satified for DDL geneeration
        # maybe model-rule check enough and nothing to add here?...
        
    def ddl():
        pass

        
class DDLGeneratorLink(object):
    
    def __init__(self, obj):
        self.obj = obj
        # test various condition that must be satified for DDL geneeration
        # maybe model-rule check enough and nothing to add here?...
        
    def ddl():
        pass

        
class DDLGeneratorSat(object):
    
    def __init__(self, obj):
        self.obj = obj
        # test various condition that must be satified for DDL geneeration
        # maybe model-rule check enough and nothing to add here?...
        
    def ddl():
        pass

        
class DDLGeneratorSatLink(object):
    
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
            raise SourceMappingError(self.obj, "'src' attribute required to generate DML".format())
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
                raise SourceMappingError(self.obj, "'src' attribute required for every 'nat_keys'")
        if self.obj.sur_key and self.obj.sur_key.get('src') is None:
            raise SourceMappingError(self.obj, "'sur_key' also required 'src' attribute for mapping (ex. a sequence: seq.nextval())")
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
                    raise SourceMappingError(self.obj, "Link sourcing inherited from Hub requires valid Hub sourcing") from e
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
                