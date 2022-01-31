from queue_controller import QueueController
from order_processor import OrderProcessor

q = QueueController()
message = q.read_message()


proc = OrderProcessor(message.order())
if proc.process():
    print("Job completed. Deleting message from queue")
    q.delete_message(message)