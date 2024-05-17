t0 = time.time()

topic_name = 'test-topic'

for i in range(10):
    message = {'number': i}
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(0.05)

t1 = time.time()

producer.flush()

t2 = time.time()

send_time = t1-t0
flush_time = t2-t1

print(f'send_time: {send_time:#.2} / flush_time: {flush_time:#.2}')
