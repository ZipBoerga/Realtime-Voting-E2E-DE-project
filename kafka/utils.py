def kafka_delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery is failed because of error: {err}')
    else:
        print(f'Message delivered for {msg.topic()} [{msg.partition()}]')
