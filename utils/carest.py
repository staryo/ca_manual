import json
import ssl
import urllib
from datetime import datetime, timedelta
from functools import partialmethod
from json import JSONDecodeError
from logging import basicConfig, DEBUG
from urllib.parse import urljoin

from requests import Session
from tqdm import tqdm
from websocket import create_connection

from base.base import Base

__all__ = [
    'CARest',
]

from utils.list_to_dict import list_to_dict

_DATETIME_SIMPLE_FORMAT = '%Y-%m-%dT%H:%M:%S'


class CARest(Base):
    KEY_ROWS = {
        'entity_route_sheet_operation': 'calculation_identity',
        'order': 'name',
        'role': 'name'
    }

    def __init__(self, login, password, base_url, ws_url, verify,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._base_url = base_url
        self._login = login
        self._password = password

        self._session = Session()
        self.ws_url = ws_url
        self._session.verify = verify

        self.cache = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()

    def _make_url(self, uri):
        return urljoin(self._base_url, uri)

    @staticmethod
    def _make_entity_name(filename, timestamp=datetime.now()):
        return '({}) {}'.format(
            timestamp.strftime(_DATETIME_SIMPLE_FORMAT),
            filename
        )

    def get_batches_with_entity_id(self, entity_id):
        return self._perform_get(
                f'rest/collection/entity_route_sheet?'
                f'order_by=start_date&asc=true&order_by=id&asc=true&'
                f'with=entity_batch&with=entity_batch.entity&'
                f'with=entity_route_sheet_operation&'
                f'filter={{ type eq 0 }} and {{ entity_batch.status eq 0 }} '
                f'and {{ entity_batch.entity_id eq {entity_id} }}'
            )

    def create_object(self, model, data):
        check_value = self.KEY_ROWS.get(model) or 'identity'
        try:
            if model == 'order_entry':
                return self._perform_get(
                    f'rest/collection/{model}?'
                    f'filter={{order_id eq {data["order_id"]} and'
                    f'entity_id eq {data["entity_id"]}}}'
                )[model][0]
            else:
                return self._perform_get(
                    f'rest/collection/{model}?filter={{{check_value} eq {data[check_value]}}}'
                )[model][0]['id']
        except KeyError:
            if model == 'order_entry':
                return self._perform_post(
                    f'rest/{model}',
                    {
                        model: [
                            data
                        ]
                    }
                )[model][0]
            else:
                return self._perform_post(
                    f'rest/{model}',
                    {
                        model: [
                            data
                        ]
                    }
                )[model][0]['id']

    def _get_from_rest_collection(self, table):
        if table not in self.cache:
            self.cache[table] = []
            self._perform_login()
            counter = 0
            step = 100000
            if table == 'specification_item':
                order_by = '&order_by=parent_id&order_by=child_id'
            elif table == 'operation_profession':
                order_by = '&order_by=operation_id&order_by=profession_id'
            elif table == 'order_entry':
                order_by = '&order_by=order_id&order_by=priority'
            elif table == 'entity_route_sheet_operation_feature_value':
                order_by = '&order_by=entity_route_sheet_operation_id&' \
                           'order_by=entity_route_sheet_operation_feature_id'
            else:
                order_by = '&order_by=id'
            pbar = tqdm(desc=f'Получение данных из таблицы {table}')
            while True:
                temp = self._perform_get(
                    f'rest/collection/{table}'
                    f'?start={counter}'
                    f'&stop={counter + step}'
                    f'{order_by}'
                )
                pbar.total = temp['meta']['count']
                counter += step
                pbar.update(min(
                    step,
                    temp['meta']['count'] - (counter - step)
                ))
                if table not in temp:
                    break
                self.cache[table] += temp[table]
                if counter >= temp['meta']['count']:
                    break
        return self.cache[table]

    def _get_main_session(self):
        return self._perform_get('action/primary_simulation_session')['data']

    def send_websocket_message(self, data: dict):
        ws = create_connection(
            f'{self.ws_url}/message',
            sslopt={'cert_reqs': ssl.CERT_NONE}
        )

        ws.send(
            json.dumps(
                {
                    "type": "CYBER_ASSISTANT_CLIENT_SOCKET_MESSAGE",
                    "data": [data]
                }
            )
        )

    def _perform_json_request(self, http_method, uri, **kwargs):
        url = self._make_url(uri)
        logger = self._logger

        logger.info('Выполнение {} запроса '
                    'по ссылке {!r}.'.format(http_method, url))

        logger.debug('Отправляемые данные: {!r}.'.format(kwargs))

        response = self._session.request(http_method,
                                         url=url,
                                         **kwargs)
        try:
            response_json = response.json()
        except JSONDecodeError:
            logger.error('Получен ответ на {} запрос по ссылке {!r}: '
                         '{!r}'.format(http_method, url, response))
            return response
            raise JSONDecodeError

        logger.debug('Получен ответ на {} запрос по ссылке {!r}: '
                     '{!r}'.format(http_method, url, response_json))
        return response_json

    _perform_get = partialmethod(_perform_json_request, 'GET')

    def _perform_post(self, uri, data):
        return self._perform_json_request('POST', uri, json=data)

    def _perform_put(self, uri, data):
        return self._perform_json_request('PUT', uri, json=data)

    def _perform_action(self, uri_part, **data):
        return self._perform_post(
            '/action/{}'.format(uri_part),
            data=data
        )

    def _perform_login(self):
        return self._perform_action(
            'login',
            data={
                'login': self._login,
                'password': self._password
            },
            action='login'
        )['data']

    def get_order(self, order_name):
        orders = self._get_from_rest_collection('order')
        for order in orders:
            if order['name'] == order_name and order['status'] in [0, 1]:
                return order
        return None

    def get_order_entry(self, order_id):
        order_entries = self._get_from_rest_collection('order_entry')
        entities = list_to_dict(self._get_from_rest_collection('entity'))
        report = []
        for order_entry in order_entries:
            if order_entry['order_id'] == order_id:
                report.append({
                    'ENTITY_ID': order_entry['entity_id'],
                    'ENTITY_IDENTITY': entities[order_entry['entity_id']]['identity'],
                    'AMOUNT': order_entry['quantity']
                })
        return report

    def stop_batch(self, identity):
        return self._perform_action(
            'entity_batch/stop',
            data={'entity_route_sheet_identity': identity}
        )

    def stop_all_batches(self, order_id):
        route_sheets = self._get_from_rest_collection('entity_route_sheet')
        batches = list_to_dict(self._get_from_rest_collection('entity_batch'))
        for batch in route_sheets:
            if batches[batch['entity_batch_id']]['order_id'] == order_id:
                self.stop_batch(batch['identity'])

    def complete_order(self, order_id):
        return self._perform_action(
            'order/complete',
            data={
                'order_id': order_id,
                'write_off': True
            }
        )

    def get_batches_to_start(self, from_date, to_date):
        from_date_query = f'{from_date.strftime("%Y-%m-%d")}T00:00:00+04:00'
        to_date_query = f'{to_date.strftime("%Y-%m-%d")}T00:00:00+04:00'
        query = f'lower_horizon_date={urllib.parse.quote(from_date_query)}&'\
                f'upper_horizon_date={urllib.parse.quote(to_date_query)}'
        return self._perform_get(
            f'data/production_plan?{query}'
        )['data']

    def start_batch(self, batch, route_sheet):
        return self._perform_action(
            f'entity_batch/start',
            data={
                'entity_batch_identity': batch,
                'entity_route_sheet_identity': route_sheet
            }
        )

    def start_task(self, route_sheet, operation):
        self._perform_action(
            f'change_entity_route_sheet_operation_status',
            data={
                'add_to_executor': False,
                'entity_route_sheet_identity': route_sheet,
                'operation_nop': operation,
                'operation_status': 1
            }
        )

    def finish_task(self, route_sheet, operation):
        self._perform_action(
            f'change_entity_route_sheet_operation_status',
            data={
                'add_to_executor': False,
                'entity_route_sheet_identity': route_sheet,
                'operation_nop': operation,
                'operation_status': 3
            }
        )

    def get_available_operations(self, department, equipment_class):
        if equipment_class is None and department is None:
            return self._perform_get(
                f"data/entity_route_sheet_operation/"
                f"list?"
                f"executable_only=true&"
                f"with=operation&"
                f"with=entity_route_sheet"
            )
        elif equipment_class is None:
            return self._perform_get(
                f"data/entity_route_sheet_operation/"
                f"list?"
                f"department_id={department}&"
                f"executable_only=true&"
                f"with=operation&"
                f"with=entity_route_sheet"
            )
        else:
            return self._perform_get(
                f"data/entity_route_sheet_operation/"
                f"list?&equipment_class_id={equipment_class}&"
                f"department_id={department}&"
                f"executable_only=true&"
                f"with=operation&"
                f"with=entity_route_sheet"
            )

    @classmethod
    def from_config(cls, config):
        return cls(
            config['login'],
            config['password'],
            config['url'],
            config['ws_url'],
            config['verify']
        )


if __name__ == '__main__':
    basicConfig(level=DEBUG)
    with CARest.from_config(
        {
            'url': 'https://b.ca-dip.kalashnikovconcern.ru',
            'login': 'qliksrv',
            'password': 'qliksrv',
            'ws_url': 'https://e123.dip.kalashnikovconcern.ru'
        }
    ) as session:

        session._perform_login()

        print(session._perform_get(
            '/rest/collection/entity_route_sheet_operation'
        ))

        # entity_id = session.create_object(
        #     model='entity',
        #     data={
        #         'identity': '336',
        #         'code': '111',
        #         'name': '444',
        #         'group': 1
        #     }
        # )
        #
        # department_id = session.create_object(
        #     'department',
        #     {
        #         'identity': '1',
        #         'name': 'Цех'
        #     }
        # )
        #
        # equipment_class_id = session.create_object(
        #     'equipment_class',
        #     {
        #         'identity': '1',
        #         'name': 'Станок'
        #     }
        # )
        #
        # operation_id = session.create_object(
        #     'operation',
        #     {
        #         'identity': '1',
        #         'name': 'Операция',
        #         'nop': '005',
        #     }
        # )
        #
        # entity_batch_id = session.create_object(
        #     'entity_batch',
        #     {
        #         'identity': f'{datetime.now().strftime("%Y%m%d%H%M%S%f")}',
        #         'entity_id': entity_id,
        #         'quantity': 5,
        #         'calculation_session_id': datetime.now().strftime('%Y%m%d'),
        #         'calculation_identity': datetime.now().strftime('%H%M%S%f'),
        #         'providing_state': 2
        #     }
        # )
        #
        # entity_route_sheet_id = session.create_object(
        #     'entity_route_sheet',
        #     {
        #         'entity_batch_id': entity_batch_id,
        #         'identity': f'{datetime.now().strftime("%Y%m%d%H%M%S%f")}',
        #         'start_date': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
        #         'stop_date': (datetime.now() + timedelta(hours=16)).strftime('%Y-%m-%dT%H:%M:%S'),
        #         'type': 0,
        #     }
        # )
        #
        # session.create_object(
        #     'entity_route_sheet_operation',
        #     {
        #         'calculation_session_id': datetime.now().strftime('%Y%m%d'),
        #         'calculation_identity': datetime.now().strftime('%H%M%S%f'),
        #         'entity_route_sheet_id': entity_route_sheet_id,
        #         'equipment_class_id': equipment_class_id,
        #         'department_id': department_id,
        #         'operation_id': operation_id
        #     }
        # )
