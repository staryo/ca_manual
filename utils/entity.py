from collections import defaultdict


class Entity(object):
    def __init__(self, code, identity, name):
        self.code = code
        self.identity = identity
        self.name = name
        self.spec = defaultdict(float)
        self.labor = 0
        self.cycle = 0
        self.department = None
        self.route = []

    def get_spec(self, report=None, amount=1):
        if not report:
            report = defaultdict(float)
        for entity in self.spec:
            if entity.product is None:
                continue
            elif entity.code == entity.product.code:
                report[entity] += self.spec[entity] * amount
            else:
                report.update(entity.get_spec(
                    report,
                    self.spec[entity] * amount
                ))
        return report