import socket
from kafka import KafkaProducer
import time

topic_name = 'ioeBattery'
server_ip = "10.9.244.8"  # Replace with your server's IP address
server_port = 502

class ConnectionPool:
    def __init__(self, server_ip, server_port, pool_size):
        self.server_ip = server_ip
        self.server_port = server_port
        self.pool_size = pool_size
        self.connections = []

        # Initialize connection pool
        for _ in range(pool_size):
            self.connections.append(self._create_connection())

    def _create_connection(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((self.server_ip, self.server_port))
        return client_socket

    def get_connection(self):
        if not self.connections:
            return self._create_connection()
        else:
            return self.connections.pop()

    def release_connection(self, connection):
        self.connections.append(connection)

    def close_all_connections(self):
        for connection in self.connections:
            connection.close()

def kafkaSendMessage(message):
    producer = KafkaProducer(bootstrap_servers=['43.205.196.66:9092'])
    producer.send(topic_name, message.encode('utf-8'))
    print(message)
    producer.flush()
    producer.close()

if __name__ == "__main__":
    pool_size = 1
    connection_pool = ConnectionPool(server_ip, server_port, pool_size)

    while True:
        client_socket = connection_pool.get_connection()

        try:
            response = client_socket.recv(10240)
        except Exception as ex:
            print(ex)
            time.sleep(10)
            continue
        finally:
            connection_pool.release_connection(client_socket)

        hex_string = ' '.join(f"{byte:02X}" for byte in response)
        
        kafkaSendMessage(hex_string)

