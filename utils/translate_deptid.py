def translate_deptid(deptid):
    return f'{deptid[:3]}{str(hex(int(deptid[3:])))[-1].upper()}'


def translate_back_deptid(deptid):
    if deptid is None:
        return None
    return f'{deptid[:3]}{str(int(deptid[-1],16)).zfill(2)}'


def translate_back_deptid_with_tire(deptid):
    if deptid is None:
        return None
    return f'{deptid[:3]}-{str(int(deptid[-1],16)).zfill(2)}'


if __name__ == '__main__':
    print(
        translate_back_deptid('104B')
    )
    print(
        translate_back_deptid_with_tire('104B')
    )
    print(
        translate_deptid('10301')
    )
