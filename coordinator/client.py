import grpc 
import twopc_pb2
import twopc_pb2_grpc
import logging
from server_list import servers
from transaction_database import transaction_data
import datetime

id = 0

def next_id():
    global id
    id +=1
    return id

def create_transaction_log(txn_id, action : str, key : str, value : int, participants=None, state="prepared", responses=None, error=None):
    if participants is None:
        participants = {}
    timestamp = datetime.datetime.now(datetime.UTC).isoformat()
    transaction_data[txn_id] =  {
        "txn_id" : txn_id,
        "action" : action,
        "key" : key,
        "value" : value,
        "state" : state,
        "participants" : [],
        "responses" : {},
        "timestamp" : timestamp,
        "error" : error        
    }

def get(stub, id, key):
    try:
        response = stub.Get(twopc_pb2.GetRequest(id=id, key=key))
        print(f"GetResponse: {response.value}")
    except  grpc.RpcError as e:
        print(f"GetResopnse: {e.details()}")
        
    
def put(stub, id, key, value):
    try:
        response = stub.Put(twopc_pb2.PutRequest(id=id, key=key, value=value))
        print("PutResponse: ", response.empty)
    except grpc.RpcError as e:
        print("PutResponse", e.details())
    

def commitQuery(stub, commit_id, transaction_data, server_id):
    
    id = next_id()
    txn_id = f"txn-{id}"
    
    vote = stub.CommitQuery(twopc_pb2.CommitQueryMessage(id=id, commit_id=commit_id))
    
    commit_txn_id = f"txn-{commit_id}"
    
    transaction_data[commit_txn_id]["responses"][f"server{server_id}"] = vote
    
    
def commit(stub, commit_id, transaction_data):
    stub.Commmit(twopc_pb2.CommitMessage(id=commit_id))
    
    commit_txn_id = f"txn-{commit_id}"
    transaction_data[commit_txn_id]["state"] = "committed"
        


def run():
    stubs = []
    for server in servers:
        host = f"localhost:{server}"
        channel = grpc.insecure_channel(host)
        stub = twopc_pb2_grpc.UpdateStub(channel)
        stubs.append(stub)

    while True:
        print(">> ", end="")
        command = input()
        if command == "get":
            print("key >>", end="")
            key = str(input())
            id = next_id()
            for i, stub in enumerate(stubs):
                print(f"Requesting 'get' from server {servers[i]}...")
                get(stub, id, key)
                continue
        elif command == "put":
            print("key >>", end="")
            key = str(input())
            print("value >>", end="")
            value = input()

            if value.isdigit():
                value = int(value)
                
            id = next_id()

            txn_id = f'txn-{id}'

            create_transaction_log(txn_id, "put", key, value)

            print(transaction_data)

            for i, stub in enumerate(stubs):
                transaction_data[f"txn-{id}"]["participants"].append(f"server{i}")
                print(f"Requesting 'put' to server {servers[i]}...")
                put(stub, id, key, value)
                commitQuery(stub, id, transaction_data, i)
                commit(stub, id, transaction_data)
                continue
            
        elif command == "exit":
            print("Exiting program.")
            break
        elif command == "log":
            print(transaction_data)
        else:
            print("Invalid command. Available commands: get, put, exit.")

    for server in servers:
        host = str('localhost:' + server)
        with grpc.insecure_channel(host) as channel:
            stub = twopc_pb2_grpc.UpdateStub(channel)
            while True:
                print(">> ", end="")
                command = input()
                if command == "get":
                    print("key >>", end="")
                    key = str(input())
                    id = next_id()
                    get(stub, id, key)
                    continue
                elif command == "put":
                    print("key >>", end="")
                    key = str(input())
                    print("value >>", end="")
                    value = input()
                    if value.isdigit():
                        value = int(value)
                    id = next_id()
                    put(stub, id, key, value)
                    continue
                elif command == "exit":
                    break
                else:
                    print("Invalid command. Available commands: get, put, exit.")
        
if __name__ == "__main__":
    logging.basicConfig()
    run()