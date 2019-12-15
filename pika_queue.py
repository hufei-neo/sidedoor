import pika

class queue():
    def __init__(self):
        self.MQ_CONFIG = {
            "host": "121.46.231.179",
            "port": '5672',
            "vhost": "test01",
            "user": "admin",
            "passwd": "eddc168168"
        }

    def connet(self):
        credentials = pika.PlainCredentials(self.MQ_CONFIG.get("user"), self.MQ_CONFIG.get("passwd"))
        parameters = pika.ConnectionParameters(self.MQ_CONFIG.get("host"), self.MQ_CONFIG.get("port"),
                                               self.MQ_CONFIG.get("vhost"),
                                               credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        l_ecom_item_key = []
        def callback(ch, method, properties, body):
            print(body)
            l_ecom_item_key.append(body)
            if len(l_ecom_item_key) == 30:
                connection.close()
        channel.basic_consume(queue='not_confirm', on_message_callback=callback, auto_ack=False)  # DEFAULT: auto_ack=True
        channel.start_consuming()
        return l_ecom_item_key

# return a list of ecom_item_key
# que = queue()
# l_ecom_item_key = que.connet()
# print(l_ecom_item_key)
