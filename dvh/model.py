import ruamel.yaml


# class Column(yaml.YAMLObject):
#     yaml_tag = '!Column'
#
#     def __init__(self, type, size=None):
#         # self.name = validate_name(name, 'Column')
#         self.type = type
#         self.size = size
#
#     def __repr__(self):
#         return '%s(type=%r, size=%r)' % (self.__class__.__name__, self.type, self.size)
#
#     def __eq__(self, other):
#         if not isinstance(other, Column):
#             return False
#         return self.type == other.size and self.size == other.size


def validate_name(col_or_table, vtype="Table"):
    emsg = None
    if not col_or_table or len(col_or_table) < 1:
        emsg = "{0} must have non-empty name".format(vtype)
    elif not col_or_table.isprintable():
        emsg = "{0} '{1}' not a valid/printable name".format(vtype, col_or_table)
    if emsg:
        raise ValueError(emsg)
    else:
        return col_or_table

# Principe mapping (class distincte): on doit ajuster le yaml uniquement, et aucun mapping n'est défini, les DDL/DML seront pris des
# valeurs par défaut et template par défaut.  On peut soit modifier ces valeurs par défaut ou encore definir de nouveau templates (autre fichier)
# et de les utiliser en les mettant en agtribut des objet yaml.
# mettre les elements (qui ont valeurs par defaut) explicites de le code en relation avec les var dans les mapping default


class ModelBase(object):

    def define_name(self, name):
        self.name = name
    
    def validate(self):
        raise NotImplementedError
        
    def validate_mapping(self):
        raise NotImplementedError
        

    # yaml.load() is not using __init__, so default attribute are generated dynamically
    def __getattr__(self, name):
        # otherwise yaml fails while accessing: data.__setstate__(state)
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
    
    
    def validate(self):
        for o in self.tables.values():
            for err in o.validate():
                yield err

    def validate_mapping(self):
        """Assumption is the object definition is satisfied (i.e. validate() is called prior to this one
        """
        for o in self.tables.values():
            for err in o.validate_mapping():
                yield err
                
                
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


class Hub(ModelBase):
    creation_order = '1'
                
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

    def validate(self):
        if self.nat_keys is None or len(self.nat_keys) == 0:
            yield "Hub must have at least one 'nat_keys'"
        if self.sur_key is None and len(self.nat_keys) > 1:
                yield "Hub without 'sur_key' can only have one 'nat_keys'"

    def validate_mapping(self):
        if self.src is None:
            yield "Hub mapping requires a 'src' attribute for sourcing"
        #assumption: validate() enforces the presence of nat_keys
        for v in self.nat_keys.values():
            if v.get('src') is None:
                yield "Hub with 'src' mapping must also specify one 'src' mapping for every 'nat_keys'"
        if self.sur_key and self.sur_key.get('src') is None:
            yield "Hub with one 'sur_key' must also specify a 'src' for its mapping (ex. for a sequence: seq.nextval())"

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
            
    def validate(self):
        if self.hubs is None or len(self.hubs) < 2:
            yield "Link must refer to more than one Hub"
        else:
            for h in self.hubs:
                if not(isinstance(h, Hub)):
                    yield "Link must refer to Hub type only"
            if self.for_keys and len(self.for_keys) != len(self.hubs):
                yield "Link's 'for_keys' mismatch the number of 'hubs'"
                                
    def validate_mapping(self):
        # mapping requires a sur_key 
        if self.sur_key.get('src') is None:
            yield "Link mapping requires 'sur_key' with a 'src' for its mapping (ex. a sequence: seq.nextval())"
        # Using default sourcing from hub
        if self.src is None:
            hub_src = set()
            for h in self.hubs:
                err_msg = list(h.validate_mapping()) 
                if len(err_msg) > 0:
                    yield "Link mapping default has invalid Hub sourcing"
                else:
                    hub_src.add(h.src)
            if len(hub_src) > 1:
                yield "Link mapping default has Hubs sourcing from different source"

                    

    def __eq__(self, other):
        if not isinstance(other, Link):
            return False
        return self.name == other.name and self.hubs == other.hubs


class Sat(ModelBase):
    creation_order = '3'

    def validate(self):
        if self.hub is None:
            yield "Satellite must refer to one 'hub'"
        if self.atts is None or len(self.atts) < 1:
            yield "Satellite must have at least one attribute in 'atts'"
        if self.lfc_dts is None:
            yield "Satellite must have a 'lfc_dts' for lifecycle date/timestamp management"

    def __eq__(self, other):
        if not isinstance(other, Sat):
            return False
        return self.name == other.name and self.atts == other.atts and self.hub == other.hub

class SatLink(ModelBase):
    creation_order = '4'

    def validate(self):
        if self.link is None:
            yield "Sat-Link must refer to one 'link'"
        if self.atts is None or len(self.atts) < 1:
            yield "Sat-Link must have at least one attribute in 'atts'"
        if self.lfc_dts is None:
            yield "Sat-Link must have a 'lfc_dts' for lifecycle date/timestamp management"

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


class Mapping(object):
    
    def __init__(self):
        pass        
        
        

