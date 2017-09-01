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


class Base(object):

    def validate(self):
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


class DVModel(Base):

    def __init__(self, tables_dic):
        self.tables = tables_dic
        self.names_ddl_order = self.names_creation_order()

    def names_creation_order(self):
        names_ordered = []
        for k, _ in sorted(self.tables.items(), key=lambda kv: kv[1].sort_order + kv[0]):
            # print("ddl order key:" + k)
            names_ordered.append(k)
        return names_ordered

    def generate_ddl(self, with_drop=False):
        if with_drop:
            for n in reversed(self.names_ddl_order):
                print("I will drop: " + n)
        for n in self.names_ddl_order:
            print("I will create DDL for: " + n)

    # def __repr__(self):
    #     return '%s(tables=%r)' % (self.__class__.__name__, self.tables)

    def __eq__(self, other):
        if not isinstance(other, DVModel):
            return False
        return self.tables == other.tables


class Hub(Base):
    sort_order = '1'

    def validate(self):
        if self.nat_keys is None or len(self.nat_keys) == 0:
            raise ValueError("Hub must have at least one 'nat_keys'")

        if self.sur_key is None:
            if len(self.nat_keys) > 1:
                raise ValueError("Hub without 'sur_key' must have a unique 'nat_keys'")
        elif len(self.sur_key.values()) > 1:
            raise ValueError("Only one 'sur_key' definition is possible")
        else:
            sk_def = list(self.sur_key.values())
            if sk_def[0].get('sequence') is None:
                raise ValueError("Hub's 'sur_key' requires a 'sequence' definition")


    def get_primarey_key(self):
        pass

    #def __repr__(self):
    #    return '%s(keys=%r, surrogate_key=%r)' % (self.__class__.__name__, self.keys, self.surrogate_key)

    def __eq__(self, other):
        if not isinstance(other, Hub):
            return False
        return self.keys == other.keys and self.surrogate_key == other.surrogate_key


class Sat(Base):
    sort_order = '2'

    def __init__(self, atts, hub):
        super().__init__()
        if not isinstance(hub, Hub):
            raise ValueError("Satellite must refer to one Hub")
        self.atts = atts
        self.hub = hub

    #def __repr__(self):
    #    return '%s(atts=%r, hub=%r)' % (self.__class__.__name__, self.atts, self.hub)

    def __eq__(self, other):
        if not isinstance(other, Sat):
            return False
        return self.atts == other.atts and self.hub == other.hub


class Link(Base):
    sort_order = '3'

    def __init__(self, hubs):
        super().__init__()
        if len(hubs) < 2 or not(isinstance(hubs[0], Hub) and isinstance(hubs[1], Hub)):
            raise ValueError("Link '{0}' must refer to two or more Hubs".format(self.name))
        self.hubs = hubs

    #def __repr__(self):
    #    return '%s(hubs=%r)' % (self.__class__.__name__, self.hubs)

    def __eq__(self, other):
        if not isinstance(other, Link):
            return False
        return self.hubs == other.hubs


class SatLink(Base):
    sort_order = '4'

    def __init__(self, atts, link):
        super().__init__()
        if not isinstance(link, Link):
            raise ValueError("SatLink must refer to one Link")
        self.atts = atts
        self.link = link

    #def __repr__(self):
    #    return '%s(atts=%r, link=%r)' % (self.__class__.__name__, self.atts, self.link)

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

