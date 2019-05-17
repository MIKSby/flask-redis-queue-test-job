import unittest
from requests import get, post, delete


class TestJobsAPI(unittest.TestCase):
    url = 'http://127.0.0.1:5000/'

    def test_post_and_delete_job_valid(self):
        r = post(f'{self.url}jobs', json=['test_item1',
                                          'test_item2',
                                          'test_item3'])
        self.assertEqual(r.status_code, 201)
        self.assertIsInstance(r.json(), dict)
        self.assertEqual(r.json()['status'], 'ok')
        self.assertIsInstance(r.json()['job_id'], str)
        d = delete(f'{self.url}jobs/{r.json()["job_id"]}')
        self.assertEqual(d.status_code, 200)
        self.assertIsInstance(d.json(), dict)
        self.assertEqual(d.json()['status'], 'ok')

    def test_get_job_valid(self):
        r = post(f'{self.url}jobs', json=['test_item1',
                                          'test_item2',
                                          'test_item3'])
        g = get(f'{self.url}jobs/{r.json()["job_id"]}')
        self.assertEqual(g.status_code, 200)
        self.assertEqual(g.json()['status'], 'ok')
        d = delete(f'{self.url}jobs/{r.json()["job_id"]}')
        self.assertEqual(d.status_code, 200)
        self.assertIsInstance(d.json(), dict)
        self.assertEqual(d.json()['status'], 'ok')

    def test_get_job_invalid(self):
        r = post(f'{self.url}jobs', json=['test_item1',
                                          'test_item2',
                                          'test_item3'])
        g = get(f'{self.url}jobs/this_job_is_not_exists')
        self.assertEqual(g.status_code, 400)
        self.assertEqual(g.json()['status'], 'error')
        d = delete(f'{self.url}jobs/{r.json()["job_id"]}')
        self.assertEqual(d.json()['status'], 'ok')

    def test_post_job_invalid(self):
        r = post(f'{self.url}jobs', json=[])
        self.assertEqual(r.status_code, 400)
        self.assertIsInstance(r.json(), dict)
        r = post(f'{self.url}jobs', json='its_not_array')
        self.assertEqual(r.status_code, 400)




