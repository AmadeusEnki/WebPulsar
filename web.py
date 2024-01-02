from flask import Flask, render_template, Response
from pulsar import Client

app = Flask(__name__)
client = Client('pulsar://localhost:6650')
topic_name = 'persistent://public/default/my_topic'
subscription_name = 'my-subscription1'

def message_listener(msg):
    return f"data: {msg.data().decode()}\n\n"

@app.route('/')
def index():
    return render_template('index.html')

def event_stream():
    try:
        consumer = client.subscribe(topic_name, subscription_name)
        while True:
            msg = consumer.receive()
            yield message_listener(msg)
            consumer.acknowledge(msg)
    except GeneratorExit:
        consumer.close()

@app.route('/stream')
def stream():
    return Response(event_stream(), content_type='text/event-stream')

if __name__ == '__main__':
    app.run(debug=True)
