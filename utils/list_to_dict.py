def list_to_dict(list_data, key_column='id'):
    report = dict()
    for row in list_data:
        report[row[key_column]] = {key: value for key, value in row.items()}

    return report
