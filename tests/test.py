import unittest
from requests import get, post, delete
import rejson


class TestJobsAPI(unittest.TestCase):
    url = 'http://127.0.0.1:5000/'
    redis = rejson.Client(host='127.0.0.1',
                          port=6379,
                          decode_responses=True)

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

    def test_post_job_invalid(self):
        r = post(f'{self.url}jobs', json=[])
        self.assertEqual(r.status_code, 400)
        self.assertIsInstance(r.json(), dict)
        r = post(f'{self.url}jobs', json='its_not_array')
        self.assertEqual(r.status_code, 400)

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
        g = get(f'{self.url}jobs/this_job_is_not_exists')
        self.assertEqual(g.status_code, 400)
        self.assertEqual(g.json()['status'], 'error')

    def test_delete_job_invalid(self):
        d = delete(f'{self.url}jobs/this_job_is_not_exists')
        self.assertEqual(d.status_code, 400)
        self.assertIsInstance(d.json(), dict)
        self.assertEqual(d.json()['status'], 'error')

    def test_get_items_valid(self):
        r = post(f'{self.url}jobs', json=['test_item1',
                                          'test_item2',
                                          'test_item3'])
        self.assertEqual(r.json()['status'], 'ok')

        it = get(f'{self.url}jobs/{r.json()["job_id"]}/items')
        self.assertEqual(it.status_code, 200)
        self.assertIn('items', it.json())
        self.assertIn('test_item1', it.json()['items'])
        self.assertIn('test_item2', it.json()['items'])
        self.assertIn('test_item3', it.json()['items'])
        self.assertEqual(len(it.json()['items']), 3)

        d = delete(f'{self.url}jobs/{r.json()["job_id"]}')
        self.assertEqual(d.json()['status'], 'ok')

    def test_get_items_invalid(self):
        r = post(f'{self.url}jobs', json=['test_item1',
                                          'test_item2',
                                          'test_item3'])
        self.assertEqual(r.json()['status'], 'ok')

        it = get(f'{self.url}jobs/this_job_is_not_exists/items')
        self.assertEqual(it.status_code, 400)
        self.assertEqual(it.json()['status'], 'error')
        self.assertNotIn('items', it.json())

        d = delete(f'{self.url}jobs/{r.json()["job_id"]}')
        self.assertEqual(d.json()['status'], 'ok')

    def test_get_next_valid(self):
        r = post(f'{self.url}jobs', json=['test_item1',
                                          'test_item2',
                                          'test_item3'])
        self.assertEqual(r.json()['status'], 'ok')

        active_it = get(f'{self.url}jobs/{r.json()["job_id"]}/items?active=true')
        self.assertEqual(active_it.status_code, 200)
        self.assertIn('test_item1', active_it.json()['items'])
        self.assertIn('test_item2', active_it.json()['items'])
        self.assertIn('test_item3', active_it.json()['items'])
        self.assertEqual(len(active_it.json()['items']), 3)

        next_it = get(f'{self.url}jobs/{r.json()["job_id"]}/next')
        active_it_new = get(f'{self.url}jobs/{r.json()["job_id"]}/items?active=true')
        self.assertNotIn(next_it.json()['item'], active_it_new.json()['items'])
        self.assertEqual(len(active_it_new.json()['items']), 2)

        d = delete(f'{self.url}jobs/{r.json()["job_id"]}')
        self.assertEqual(d.json()['status'], 'ok')
        self.redis.delete(f'hold_{next_it.json()["item"]}')
