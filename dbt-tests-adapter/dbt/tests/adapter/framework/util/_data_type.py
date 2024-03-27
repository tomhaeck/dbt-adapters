"""
Use these classes in assert calls to test data types
"""


class AnyFloat:
    def __eq__(self, other):
        return isinstance(other, float)


class AnyInteger:
    def __eq__(self, other):
        return isinstance(other, int)


class AnyString:
    def __eq__(self, other):
        return isinstance(other, str)


class AnyStringWith:
    def __init__(self, contains=None):
        self.contains = contains

    def __eq__(self, other):
        if not isinstance(other, str):
            return False
        if self.contains is None:
            return True
        return self.contains in other

    def __repr__(self):
        return "AnyStringWith<{!r}>".format(self.contains)
