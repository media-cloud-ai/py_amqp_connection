
Python AMQP Connection
======================

To connect worker to an AMQP instance


Build & Installation
--------------------

### Build only
```bash
python3 setup.py build
```

### Build & local install
```bash
python3 setup.py install
```

Usage
-----
```python
#!/usr/bin/env python

from amqp_connection import Connection

conn = Connection()

def callback(ch, method, properties, body):
    # process the consumed message
    message = body.decode('utf-8')
    if message == 'Hello':
        # produce a respone
        conn.publish_json('out_queue', "OK")
        # Acknowledge the message (ACK)
        return True
    # Requeue the message (NACK)
    return False

config = {
	"username": "amqp_user",
	"password": "amqp_password",
	"vhost": "vhost_name",
	"hostname": "localhost",
	"port": 12345
}

conn.run(config, 'in_queue', ['out_queue'], callback)

```
