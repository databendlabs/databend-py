import unittest


class Dict(dict):
    def __init__(self, **kw):
        super().__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value


class TestDict(unittest.TestCase):
    databend_url = None

    @classmethod
    def setUpClass(cls):
        cls.databend_url = "test_url"

    def test_init(self):
        d = Dict(a=1, b="test")
        self.assertEqual(self.databend_url, "test_url")
        self.assertEqual(d.a, 1)
        self.assertEqual(d.b, "test")
        self.assertTrue(isinstance(d, dict))

    def test_key(self):
        d = Dict()
        d["key"] = "value"
        self.assertEqual(d.key, "value")


if __name__ == "__main__":
    unittest.main()
