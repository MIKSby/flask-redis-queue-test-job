import uuid

from flask import Flask, request
from flask_restful import Api, Resource, output_json
from rejson import Client, Path

app = Flask(__name__)
api = Api(app)


class JobsAPI(Resource):
    def __init__(self):
        self.redis = Client(host='127.0.0.1',
                            port=6379,
                            decode_responses=True)

    def get(self):
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


api.add_resource(JobsAPI, '/jobs', '/jobs/<string:job_id>')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)
