
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
    print(body.decode('utf-8'))

    # produce a respone
    conn.sendJson('out_queue', "OK")

config = {
	"username": "amqp_user",
	"password": "amqp_password",
	"vhost": "vhost_name",
	"hostname": "localhost",
	"port": 12345
}

conn.load_configuration(config)
conn.connect([
    'in_queue',
    'out_queue'
])
conn.consume('in_queue', callback)

```
