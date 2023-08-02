import csv
import json
import os
import sys
from xml.etree.ElementTree import ParseError, XMLParser
from xml.etree.ElementTree import parse

from tqdm import tqdm

from utils.xml_tools import get_text_value, get_float_value, \
    get_float_value_with_dot
from utils.entity import Entity


class ReadSession(object):
    def __init__(self):
        self.entities = {}

    def add_entity(self, code, identity, name):
        code = code.zfill(18)
        if code not in self.entities:
            self.entities[code] = Entity(
                code=code,
                identity=identity,
                name=name
            )
        if self.entities[code].identity is None:
            self.entities[code].identity = identity
            self.entities[code].name = name
        return self.entities[code]

    def read_from_folder(self, path, pattern):
        list_of_files = os.listdir(path)
        # Iterate over all the entries
        if len(list_of_files) > 100:
            iter = tqdm(sorted(list_of_files), desc=path, file=sys.stdout)
        else:
            iter = sorted(list_of_files)

        for entry in iter:
            # Create full path
            full_path = os.path.join(path, entry)
            # If entry is a directory then get the list
            # of files in this directory
            if os.path.isdir(full_path):
                self.read_from_folder(full_path, pattern)
            else:
                if pattern in entry:
                    # tqdm.write(full_path)
                    with open(full_path, mode='r', encoding='utf-8') as file:
                        answer = self.read_from_file(
                            file
                        )
                        if answer == {}:
                            tqdm.write(full_path)

    def read_from_file(self, xml_file):
        # парсим xml
        try:
            # ставим utf-8 хардкодом, чтоб никаких неожиданностей не было
            xmlp = XMLParser(encoding="utf-8")
            tree = parse(xml_file, parser=xmlp)
            root = tree.getroot()
        except ParseError:
            tqdm.write('Ошибка чтения файла {}'
                       ' -- не распознан корень'.format(xml_file))
            return {}

        report = {'result': 'good'}

        # ищем\
        material = root.find('MATERIALDATA')

        if get_text_value(material, 'STATUS') == 'Z4':
            return report
        if material is None:
            tqdm.write('Ошибка чтения файла -- не распознан MATERIALDATA')
            return {}

        parent = self.add_entity(
            code=get_text_value(material, 'MATNR').zfill(18),
            identity=get_text_value(material, 'MEINS'),
            name=get_text_value(material, 'MAKTX'),
        )

        bom = root.find('BOMDATA')

        if bom:
            bom_items = bom.findall('BOMITEM')
            base_quantity = get_float_value(bom.find('BOMHEADER'), 'BASE_QUAN')
            items = set()
            for item in bom_items:
                if get_text_value(item, 'ISSUE_LOC') is not None:
                    department = get_text_value(item, 'ISSUE_LOC')
                    if parent.department is None:
                        parent.department = department
                if get_text_value(item, 'AI_GROUP') is not None:
                    if get_float_value(item, 'USAGE_PROB') == 0:
                        continue
                if get_text_value(item, 'ITEM_CATEG') in ['X']:
                    if get_text_value(item, 'COMPONENT') in items:
                        tqdm.write(
                            'Дубль номенклатуры {} в спецификации {}'.format(
                                get_text_value(item, 'COMPONENT'),
                                get_text_value(material, 'MATNR')
                            )
                        )
                    else:
                        items.add(get_text_value(item, 'COMPONENT'))
                if get_text_value(item, 'ITEM_CATEG') in ['O', 'U', 'Y']:
                    continue
                child = self.add_entity(
                    code=get_text_value(item, 'COMPONENT'),
                    identity=None,
                    name=None,
                )
                try:
                    parent.spec[child] = get_float_value_with_dot(
                        item, 'COMP_QTY'
                    ) / base_quantity
                except ValueError:
                    tqdm.write(
                        'Ошибка чтения количества в спецификации, '
                        'PARENT {}, CHILD {}'.format(
                            parent.code, child.code
                        )
                    )
        route = root.find('ROUTINGDATA')
        if '1354D0000015-A0721' in parent.code:
            a = 1

        total_labor = 0
        cycle = 0
        if route:
            route_operations = route.findall('ROUTINGOPERATION')
            for operation in route_operations:
                op_name = get_text_value(operation, 'LTXA1') or 'БЕЗ НАЗВАНИЯ'
                if 'КОНТРОЛЬ' in \
                        op_name.upper():
                    continue
                if 'МАГНИТОПОРОШКОВАЯ ДЕФЕКТОСКОПИЯ' in \
                        op_name.upper():
                    continue
                if 'КЛЕЙМЕНИЕ ОТК' in \
                        op_name.upper():
                    continue

                if '9999' in get_text_value(operation, 'VORNR'):
                    continue
                if '9990' in get_text_value(operation, 'VORNR'):
                    cycle = get_float_value_with_dot(
                            operation, 'VGW01'
                        ) * 60
                    continue
                parent.route.append({
                    'IDENTITY': f"{get_text_value(material, 'MATNR')}_"
                                f"{get_text_value(material, 'VERID')}_"
                                f"{get_text_value(operation, 'VORNR')}",
                    'NOP': get_text_value(operation, 'VORNR'),
                    'NAME': op_name,
                    'T_SHT': round(get_float_value_with_dot(
                        operation, 'VGW03') / get_float_value_with_dot(
                        operation, 'BMSCH') * get_float_value_with_dot(
                        operation, 'VGW06') / 100 * 60, 4),
                    'EQUIPMENT_ID': get_text_value(operation, 'ARBID')
                })
                total_labor += get_float_value_with_dot(
                    operation, 'VGW03') / get_float_value_with_dot(
                    operation, 'BMSCH') * get_float_value_with_dot(
                    operation, 'VGW06') / 100 * 60

        parent.labor = round(total_labor * 10000) / 10000
        parent.cycle = round(cycle * 10000) / 10000

        return report


def get_entity_routes(path):
    session = ReadSession()
    session.read_from_folder(
        path,
        '_'
    )
    parents_dict = {'SPEC': {}, 'ENTITY': {}, 'ROUTE': {}}
    for entity in session.entities.values():
        if entity.department != '1072':
            continue
        parents_dict['ENTITY'][entity.code] = {
            'CODE': entity.code,
            'IDENTITY': entity.identity,
            'NAME': entity.name,
            'LABOR': entity.labor,
            'CYCLE': entity.cycle
        }
        parents_dict['ROUTE'][entity.code] = entity.route
        for child in entity.spec:
            if child.code not in parents_dict['SPEC']:
                parents_dict['SPEC'][child.code] = {}
            parents_dict['SPEC'][child.code][entity.code] = entity.spec[child]
            parents_dict['ENTITY'][child.code] = {
                'CODE': child.code,
                'IDENTITY': child.identity,
                'NAME': child.name,
                'LABOR': child.labor,
                'CYCLE': child.cycle
            }

    return parents_dict


if __name__ == '__main__':
    with open('routes.json', 'w', encoding='utf-8') as output_file:
        json.dump(
            get_entity_routes('/media/work/SECOND/Исходные данные/KK/1feb'),
            output_file
        )

