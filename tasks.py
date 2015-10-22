from celery import Celery
from celery import group
from celery.result import GroupResult
from celery.result import ResultBase, AsyncResult
import time
import os
import swiftclient.client
import json




app = Celery('tasks', backend='amqp', broker='amqp://guest@localhost//')


config = {'user':os.environ['OS_USERNAME'],
                  'key':os.environ['OS_PASSWORD'],
                  'tenant_name':os.environ['OS_TENANT_NAME'],
                  'authurl':os.environ['OS_AUTH_URL']}
conn = swiftclient.client.Connection(auth_version=2, **config)



@app.task()
def div(bucket_name):
	resp, obj_list = conn.get_container(bucket_name)

	mTasks = group(getNumberOfMentions.s(bucket_name, obj_list[i]['name']) for i in xrange(0,len(obj_list)-1))()
	while not mTasks.ready():
		time.sleep(5)
		print mTasks.completed_count()
	print mTasks.get()
	return mTasks.get()

@app.task()
def getNumberOfMentions(bucket_name, fileName):
	print fileName
	data = []
	itterator = 0
	if not os.path.isfile(fileName):
		print "Downloding file: " + fileName
		resp, mObject = conn.get_object(bucket_name, fileName)
		with open(fileName, 'w+') as f:
			f.write(mObject)

	print 2		
	with open(fileName, 'r') as f:
		print 3
		for line in f:
			if itterator % 2 == 0:
				data.append(json.loads(line))
			itterator +=1
		return count(data)
	return 0


def count(tweets):
	cHan = 0
	cHon = 0
	cHen = 0
	cDen = 0
	cDet = 0
	cDenna = 0
	cDenne = 0
	for tweet in tweets:
		text = tweet['text'].lower()
		cHan += text.count('han')
		cHon += text.count('hon')
		cHen += text.count('hen')
		cDen += text.count('den')
		cDet += text.count('det')
		cDenna += text.count('denna')
		cDenne += text.count('denne')

	return [cHan, cHon, cHen, cDen, cDet, cDenna, cDenne]
