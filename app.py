from flask import Flask
import os
import swiftclient.client
import json
import time
from tasks import div
from celery.result import ResultBase, AsyncResult



app = Flask(__name__)


config = {'user':os.environ['OS_USERNAME'],
                  'key':os.environ['OS_PASSWORD'],
                  'tenant_name':os.environ['OS_TENANT_NAME'],
                  'authurl':os.environ['OS_AUTH_URL']}
conn = swiftclient.client.Connection(auth_version=2, **config)
bucket_name = "tweets"


@app.route('/')
def getJson():
	data = []
	with open("tweets_19.txt", 'r') as f:
		itterator = 0
		for line in f:
			if itterator % 2 == 0:
				data.append(json.loads(line))
			itterator +=1
		return json.dumps(data[1]['text'])

	return "Hej"


@app.route('/han')
def han():
	numbers = div.delay(bucket_name)
	while not numbers.ready():
		time.sleep(5)
		print "Not Ready BIG"
	print numbers.get()
	return "hehe"


if __name__ == '__main__':
    app.run(host = '0.0.0.0', debug=True)