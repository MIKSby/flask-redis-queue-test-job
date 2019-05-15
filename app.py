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
        return {'status': 'ok',
                'jobs': [i for i in self.redis.keys()]}

    def post(self):
        if isinstance(request.json, list):
            job_id = str(uuid.uuid4())

            data = {
                job_id: {
                    'items': request.json,
                    'done': []
                }
            }

            if self.redis.jsonset(job_id, Path.rootPath(), data):
                return output_json({'status': 'ok',
                                    'description': 'Job is added to queue.',
                                    'job_id': job_id}, 201)
        else:
            return output_json({'status': 'error',
                                'description': 'Wrong request!'}, 400)


api.add_resource(JobsAPI, '/jobs')


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)
