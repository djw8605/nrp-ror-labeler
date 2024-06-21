import unittest
from unittest.mock import patch, MagicMock
from main import convert_ror_to_osg, convert_url_to_k8s_value

class TestMain(unittest.TestCase):
    def test_convert_ror_to_osg(self):
        url = "https://ror.org/05gztc26"
        expected = "https://osg-htc.org/iid/05gztc26"
        self.assertEqual(convert_ror_to_osg(url), expected)

    def test_convert_url_to_k8s_value(self):
        url = "https://ror.org/05gztc26"
        expected = "ror.org_05gztc26"
        self.assertEqual(convert_url_to_k8s_value(url), expected)

        url = "https://osg-htc.org/iid/05gztc26"
        expected = "osg-htc.org_iid_05gztc26"
        self.assertEqual(convert_url_to_k8s_value(url), expected)





if __name__ == '__main__':
    unittest.main()