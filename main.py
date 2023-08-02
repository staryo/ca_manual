import argparse
from argparse import ArgumentParser
from os import getcwd
from os.path import join

import urllib3

from logic.import_data_to_ca import start_batches
from utils.carest import CARest
from utils.excel import excel_to_dict
from utils.list_to_dict import list_to_dict
from utils.yml_config import read_config


def main():
    # reading execution parameters
    urllib3.disable_warnings()

    parser = ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        # description=DESCRIPTION
    )

    parser.add_argument('-c', '--config', required=False,
                        default=join(getcwd(), 'config.yml'))
    parser.add_argument('-f', '--file', required=True)
    parser.add_argument('-d', '--debug', required=False, action='store_true',
                        default=False)

    args = parser.parse_args()

    config = read_config(args.config)

    # reading excel
    input_data = excel_to_dict(args.file)
    KEY_COLUMNS = {
        'batches': 'BATCH_ID',
        'operations': 'ID',
        'routes': 'ROUTE_ID',
        'bill_of_materials': 'PARENT_CODE|CODE',
        'departments': 'DEPT_ID',
        'equipment': 'ID',
    }

    data_to_import = {}
    for column, name in config['columns'].items():
        if name in input_data:
            data_to_import[column] = list_to_dict(
                input_data[name],
                key_column=KEY_COLUMNS[column]
            )
        else:
            data_to_import[column] = []

    # starting batches
    with CARest.from_config(config['CA']) as session:
        session._perform_login()
        start_batches(session, data_to_import)


if __name__ == '__main__':
    main()
