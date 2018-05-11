from redis import StrictRedis
from rq import Queue, get_failed_queue
import time

MAX_ATTEMPTS = 24*60

queue_conn = StrictRedis(host='433-19.csse.rose-hulman.edu', port=6379, db=0)
q = Queue(connection=queue_conn)

while True:
    failed_queue = get_failed_queue(queue_conn)
    dictionary = {}
    dictionary2 = {}
    print(failed_queue.count)
    with open("Queue_Log.txt", 'r+') as f:
        for line in f:
            key, val = line.split()
            dictionary[str(key)] = int(val)

        for job_id in failed_queue.job_ids:
            if job_id in dictionary.keys():
                dictionary2[job_id] = dictionary[job_id]+1
            else:
                dictionary2[job_id] = 1

            if dictionary2[job_id] <= MAX_ATTEMPTS:
                failed_queue.requeue(job_id)
            else:
                dictionary2.pop(job_id)

        f.seek(0)
        for key in dictionary2.keys():
            f.write(key+","+dictionary2.get(key))

        f.truncate()
        f.close()

    failed_queue.empty()
    time.sleep(60)
