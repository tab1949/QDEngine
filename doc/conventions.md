# Conventions

## Communication Between QDEngine & Client
Before all, messages must/will follow the specifications:  
(Server -> Client)  
```json
{
    "code": 1, // signed integer, 64 bit
    "msg": "", // string
    "data": {} // depends on "code"
}
```
- When `"code"` < 0, it is an error code, and `"msg"` is the error information.  
  - `"code"` will be `0` when a connection is successfully established.
- `"data"` has no fixed structure. Server messages with different `"code"` may have different structures of `"data"`.  

(Client -> Server)
```json
{
    "action": "Connect", // action name
    "data": {} // depends on "action"
}
```  
The communication can be split into several steps:  

### 1. Handshake (after WebSocket connection established)
No matter what the client sent, QDEngine server will send a greeting message like:
```json
{
    "code": 1,
    "msg": "connected",
    "data": {
        "token": "abcd1234" // client should save this token, it will be used in later instructions
    }
}
```
And then the client should send the token back:  
```json
{
    "action": "Handshake",
    "data": {
        "token": "abcd1234" // MUST be same to the token from server
    }
}
```
After that communication above, a connection is completely established. 