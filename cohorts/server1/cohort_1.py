import twopc_pb2
import twopc_pb2_grpc
import grpc
import logging
from concurrent import futures
from database import data
from transaction_database import transaction_data
import datetime

def create_transaction_log(txn_id, action : str, key : str, value : int, participants=None, state="prepared", error=None):
    if participants is None:
        participants = {}
    timestamp = datetime.datetime.now(datetime.UTC).isoformat()
    transaction_data[txn_id] =  {
        "txn_id" : txn_id,
        "action" : action,
        "key" : key,
        "value" : value,
        "state": state,
        "participants" : [],
        "responses" : {},
        "timestamp" : timestamp,
        "error" : error        
    }

class UpdateServicer(twopc_pb2_grpc.UpdateServicer):
    def Get(self, getRequest, context):
        
        for key_local, value_local in data["commited"].items():
            if getRequest.key == key_local:
                return twopc_pb2.GetResponse(value=value_local)
            else:
                print(f"there is not the value of {getRequest.key} in data")
        print(f"sent the value of {getRequest.key} to client")
    
    def Put(self, putRequest, context):

        txn_id = f"txn-{putRequest.id}"

        for key_local, value_local in data["commited"].items():
            if putRequest.key == key_local:
                return twopc_pb2.PutResponse(empty="This key is already registered.")
            if putRequest.value == value_local:
                return twopc_pb2.PutResponse(empty="This value is already registered.")

        try:
            data["prepared"][putRequest.key] = putRequest.value
            create_transaction_log(txn_id, "put", putRequest.key, putRequest.value)
            print(transaction_data)
            print(data)
            return twopc_pb2.PutResponse(empty="registration has been successfully done")
        except:
            create_transaction_log(txn_id, "put", putRequest.key, putRequest.value, None, "prepared", grpc.RpcError.details())
            print(data)
            return twopc_pb2.PutResponse(empty="something is wrong")
        
    def CommitQuery(self, request, context):
        create_transaction_log(request.id, "rcvCommitQuery", None, None, None, None)
        
        txn_id = f"txn-{request.commit_id}"
        
        if transaction_data[txn_id]["state"] == "prepared":
            return twopc_pb2.CommitQueryResponse(ack="YES") 
        
    def Commmit(self, request, context):
        # commit自体のidを渡すの忘れた
        # create_transaction_log()
        
        txn_id = f"txn-{request.id}"
        
        key = transaction_data[txn_id]["key"]
        value = transaction_data[txn_id]["value"]
        
        data["commited"][key] = value
        del data["prepared"][key]
        print("commit done")
        return twopc_pb2.CommitResponse(ack="DONE")
    
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    twopc_pb2_grpc.add_UpdateServicer_to_server(
        UpdateServicer(), server
    )
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()

