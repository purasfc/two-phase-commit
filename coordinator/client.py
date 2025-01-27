import grpc 
import twopc_pb2
import twopc_pb2_grpc
import logging
from server_list import servers

id = 0

def next_id():
    global id
    id +=1
    return id

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
    
    
def two_phase_commit(stub, commit_id):
    
    vote_yes_num = 0
    
    id = next_id()
    
    vote = stub.CommitQuery(twopc_pb2.CommitQueryMessage(id=id, commit_id=commit_id))
    
    if vote == "yes":
        vote_yes_num += 1
    
    id = next_id()
    
    if vote_yes_num == len(servers):
        stub.Commit(twopc_pb2.CommitMessage(id=id))
    
    


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
            for i, stub in enumerate(stubs):
                print(f"Requesting 'put' to server {servers[i]}...")
                put(stub, id, key, value)
                continue
        elif command == "exit":
            print("Exiting program.")
            break
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