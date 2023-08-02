from collections import defaultdict
from datetime import datetime

from tqdm import tqdm


def start_batches(ca_session, data):
    spec = defaultdict(dict)
    department_ids = {}
    for row in tqdm(data['departments'].values(), desc='Создаем подразделения'):
        department_ids[row['DEPT_ID']] = ca_session.create_object(
            model='department',
            data={
                'identity': str(row['DEPT_ID']),
                'name': str(row['NAME'])
            }
        )
    entities_ids = {}
    for row in tqdm(data['bill_of_materials'].values(), desc='Создаем ДСЕ'):
        spec[row['PARENT_CODE']][row['CODE']] = row['AMOUNT']
        entities_ids[row['PARENT_CODE']] = ca_session.create_object(
            model='entity',
            data={
                'identity': str(row['PARENT_CODE']),
                'code': str(row['PARENT_IDENTITY']),
                'name': str(row['PARENT_NAME']),
                'group': 1
            }
        )
        entities_ids[row['CODE']] = ca_session.create_object(
            model='entity',
            data={
                'identity': str(row['CODE']),
                'code': str(row['IDENTITY']),
                'name': str(row['NAME']),
                'group': 1
            }
        )
    routes = defaultdict(list)
    for row in data['operations'].values():
        routes[row['ROUTE_ID']].append(row)
    equipment_class = {}
    for row in data['equipment'].values():
        equipment_class[row['EQUIPMENT_ID']] = row

    start_date = datetime.now()
    order_id = ca_session.create_object(
        model='order',
        data={
            'name': "ORDER",
            'priority': 1,
            'start_date': start_date.strftime('%Y-%m-%dT%H:%M:%S'),
            'stop_date': start_date.strftime('%Y-%m-%dT%H:%M:%S')
        }
    )

    # ca_session.create_object(
    #     model='order_entry',
    #     data={
    #         'order_id': order_id,
    #         'entity_id': entity_id,
    #         'quantity': amount,
    #         'priority': 1
    #     }
    # )

    for batch_id, batch_data in tqdm(data['batches'].items(), desc='Запускаем партии'):
        calc_session_id = datetime.now().strftime('%Y%m%d')
        entity_batch_id = ca_session.create_object(
            'entity_batch',
            {
                'identity': str(batch_id),
                'entity_id': entities_ids[
                    data['routes'][
                        batch_data['ROUTE_ID']
                    ]['CODE']
                ],
                'quantity': batch_data['AMOUNT'],
                'calculation_session_id': calc_session_id,
                'calculation_identity': str(batch_id),
                'providing_state': 2,
                'order_id': order_id
            }
        )
        entity_route_sheet_id = ca_session.create_object(
            'entity_route_sheet',
            {
                'entity_batch_id': entity_batch_id,
                'identity': str(batch_id),
                'start_date': start_date.strftime('%Y-%m-%dT%H:%M:%S'),
                'stop_date': batch_data['DATE_TO'].strftime('%Y-%m-%dT%H:%M:%S'),
                'type': 0,
            }
        )

        for operation in routes[batch_data['ROUTE_ID']]:
            try:
                equipment_class_id = ca_session.create_object(
                    'equipment_class',
                    {
                        'identity': str(equipment_class[operation['EQUIPMENT_ID']][
                            'EQUIPMENT_ID']),
                        'name': str(equipment_class[operation['EQUIPMENT_ID']]['NAME'])
                    }
                )
            except KeyError:
                equipment_class_id = ca_session.create_object(
                    'equipment_class',
                    {
                        'identity': str(operation['EQUIPMENT_ID']),
                        'name': f"ИД {operation['EQUIPMENT_ID']}"
                    }
                )

            operation_id = ca_session.create_object(
                'operation',
                {
                    'identity': str(operation['ID']),
                    'name': f"{operation['NAME']} ({operation['NORM_AMOUNT']})",
                    'nop': operation['NOP'],
                }
            )

            ca_session.create_object(
                'entity_route_sheet_operation',
                {
                    'calculation_session_id': calc_session_id,
                    'calculation_identity': f"{batch_id}_{operation['NOP']}",
                    'entity_route_sheet_id': entity_route_sheet_id,
                    'equipment_class_id': equipment_class_id,
                    'department_id': department_ids[operation['DEPT_ID']],
                    'operation_id': operation_id,
                    'start_date': start_date.strftime('%Y-%m-%dT%H:%M:%S'),
                    'stop_date': start_date.strftime('%Y-%m-%dT%H:%M:%S'),
                    'prod_time': round(8 / operation['NORM_AMOUNT'] * 60, 4)
                }
            )

        ca_session.send_websocket_message(
            {
                "event": "SHEET_STARTED",
                "data": {
                    "sheetId": entity_route_sheet_id,
                    "sheetIdentity": batch_id
                }
            }
        )