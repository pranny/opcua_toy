


`curl -H 'Content-Type: application/json' -X POST http://0.0.0.0:8080/remove_connector -d '{"uids":[1]}'`

`curl -H 'Content-Type: application/json' -X POST http://0.0.0.0:8080/connector -d '{"connector":[{"url":"opc.tcp://Pranavs-MacBook-Pro-2.local:53530/OPCUA/SimulationServer","node_ids": ["ns=3;i=1001","ns=3;i=1002"], "uid": 1}]}'`