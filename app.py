import uuid

from flask import Flask, request
from flask_restful import Api, Resource, output_json, reqparse
from rejson import Client, Path

app = Flask(__name__)
api = Api(app)


class JobsAPI(Resource):
    def __init__(self):
        self.redis = Client(host='127.0.0.1',
                            port=6379,
                            decode_responses=True)

    def get(self, **kwargs):
        if kwargs.get('job_id'):
            job_id = kwargs.get('job_id')
            if self.redis.exists(job_id):
                parser = reqparse.RequestParser()

                if request.url_rule.rule == '/jobs/<string:job_id>/next':
                    if self.redis.jsonget(job_id, Path('.items')):
                        item = self.redis.jsonarrpop(job_id, Path('.items'))
                        return output_json({'status': 'ok',
                                            'job_id': job_id,
                                            'item': item}, 200)
                    self.redis.delete(job_id)
                    return output_json({'status': 'error',
                                        'job_id': job_id,
                                        'description': 'The items list is empty.'
                                                       'Job will be removed.'}, 400)

                if request.url_rule.rule == '/jobs/<string:job_id>/items':
                    parser.add_argument('active',
                                        default='true',
                                        choices=('true', 'false'))
                    args = parser.parse_args(strict=True)

                    if args.get('active') == 'true':
                        items = self.redis.jsonget(job_id, Path('.items'))
                        if not items:
                            self.redis.delete(job_id)
                            return output_json({'status': 'error',
                                                'job_id': job_id,
                                                'description': 'The items list is empty.'
                                                               'Job will be removed.'}, 400)
                        return output_json({'status': 'ok',
                                            'job_id': job_id,
                                            'items': items}, 200)
                    items = self.redis.jsonget(job_id, Path('.done'))
                    return output_json({'status': 'ok',
                                        'job_id': job_id,
                                        'items': items}, 200)
            else:
                return output_json({'status': 'error',
                                    'job_id': job_id,
                                    'description': 'The job is not in the queue.'}, 400)

        return output_json({'status': 'ok',
                            'jobs': [i for i in self.redis.keys()]}, 200)

    def post(self):
        if isinstance(request.json, list) and request.json:
            job_id = str(uuid.uuid4())

            data = {
                'items': request.json,
                'done': []
            }

            if self.redis.jsonset(job_id, Path.rootPath(), data):
                return output_json({'status': 'ok',
                                    'description': 'Job is added to queue.',
                                    'job_id': job_id}, 201)
        else:
            return output_json({'status': 'error',
                                'description': 'Wrong request!'}, 400)

    def delete(self, job_id):
        if self.redis.exists(job_id):
            self.redis.delete(job_id)
            return output_json({'status': 'ok',
                                'description': 'Job is deleted.'}, 200)
        else:
            return output_json({'status': 'error',
                                'description': 'The job is not in the queue.'}, 200)


api.add_resource(JobsAPI,
                 '/jobs',
                 '/jobs/<string:job_id>',
                 '/jobs/<string:job_id>/next',
                 '/jobs/<string:job_id>/items')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)
