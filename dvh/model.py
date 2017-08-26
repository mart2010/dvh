import yaml


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


class DVModel(yaml.YAMLObject):
    yaml_tag = '!DVModel'

    def __init__(self, tables_dic):
        self.tables = tables_dic
        self.names_ddl_order = self.names_creation_order()

    def names_creation_order(self):
        names_ordered = []
        for k, _ in sorted(self.tables.items(), key=lambda kv: str(kv[1].sort_order) + kv[0]):
            # print("ddl order key:" + k)
            names_ordered.append(k)
        return names_ordered

    def generate_ddl(self, with_drop=False):
        if with_drop:
            for n in reversed(self.names_ddl_order):
                print("I will drop: " + n)
        for n in self.names_ddl_order:
            print("I will create DDL for: " + n)

    def __repr__(self):
        return '%s(tables=%r)' % (self.__class__.__name__, self.tables)



class Column(yaml.YAMLObject):
    yaml_tag = '!Column'

    def __init__(self, type, size=None):
        # self.name = validate_name(name, 'Column')
        self.type = type
        self.size = size

    def __repr__(self):
        return '%s(type=%r, size=%r)' % (self.__class__.__name__, self.type, self.size)


class Base(yaml.YAMLObject):

    # probably a clever way to implement at this level..
    def ddl_expression(self):
        return "ddl " + self._repr__()


class Hub(Base):
    yaml_tag = '!Hub'
    sort_order = 1

    def __init__(self, keys, surrogate_key=None):
        super().__init__()
        if len(keys) < 1:
            raise ValueError("Hub must have at least one natural key")
        self.keys = keys
        if surrogate_key:
            if surrogate_key.get('name') is None or surrogate_key.get('sequence') is None:
                raise ValueError("Surrogate definition requires both a name and a sequence")
        self.surrogate_key = surrogate_key

    def get_primarey_key(self):
        pass

    def __repr__(self):
        return '%s(keys=%r, surrogate_key=%r)' % (self.__class__.__name__, self.keys, self.surrogate_key)


class Sat(Base):
    yaml_tag = '!Sat'
    sort_order = 2

    def __init__(self, atts, hub):
        super().__init__()
        if not isinstance(hub, Hub):
            raise ValueError("Satellite must refer to one Hub")
        self.atts = atts
        self.hub = hub

    def __repr__(self):
        return '%s(atts=%r, hub=%r)' % (self.__class__.__name__, self.atts, self.hub)


class Link(Base):
    yaml_tag = '!Link'
    sort_order = 3

    def __init__(self, hubs):
        super().__init__()
        if len(hubs) < 2 or not(isinstance(hubs[0], Hub) and isinstance(hubs[1], Hub)):
            raise ValueError("Link '{0}' must refer to two or more Hubs".format(self.name))
        self.hubs = hubs

    def __repr__(self):
        return '%s(hubs=%r)' % (self.__class__.__name__, self.hubs)


class SatLink(Base):
    yaml_tag = '!SatLink'
    sort_order = 4

    def __init__(self, atts, link):
        super().__init__()
        if not isinstance(link, Link):
            raise ValueError("SatLink must refer to one Link")
        self.atts = atts
        self.link = link

    def __repr__(self):
        return '%s(atts=%r, link=%r)' % (self.__class__.__name__, self.atts, self.link)



h1 = Hub({'id': {'type': 'number', 'size': '(3,2)'}})
h2 = Hub({'id': {'type': 'number', 'size': '(9)'}})
s11 = Sat({'att1': {'type': 'number', 'size': '(3,2)'}, 'att2': {'type': 'varchar', 'size': '(15)'}}, h1)
s12 = Sat({'att1': {'type': 'number', 'size': '(10)'}, 'att2': {'type': 'varchar', 'size': '(15)'}}, h1)
s21 = Sat({'attx': {'type': 'number', 'size': '(10)'}, 'atty': {'type': 'varchar', 'size': '(15)'}}, h2)

l = Link([h1, h2])
sl1 = SatLink({'attt': {'type': 'date'}}, l)

t_d = DVModel({'sl1': sl1, 's21': s21, 'h2': h2, 'h1': h1, 's11': s11, 's12': s12, 'l': l})

print(yaml.dump(t_d))

print(t_d.generate_ddl(with_drop=False))
