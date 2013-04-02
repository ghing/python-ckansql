import unittest
import ckansql

class IntegrationTest(unittest.TestCase):
    def setUp(self):
        self.url = "http://ods-service-test.herokuapp.com/api/action/datastore_search_sql"
        self.conn = ckansql.connect(self.url)

    def test_select(self):
        curr = self.conn.cursor()
        curr.execute("SELECT * FROM appropriations LIMIT 2")
        rows = curr.fetchall()
        self.assertEqual(len(rows), 2)

    def test_select_with_parameters(self):
        curr = self.conn.cursor()
        curr.execute("SELECT * FROM appropriations WHERE \"FUND TYPE\" = '%(fund_type)s' LIMIT 2", {
            'fund_type': 'LOCAL',
        })
        rows = curr.fetchall()
        self.assertEqual(len(rows), 2)
