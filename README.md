Toy example to perform OPC UA Read using Async Methods

- Exposes an Async based OPCConnector class. It prints the data to console, but can also write to file. If written to file, the output can be verified by the verify.py script. It uses PySpark to combine the multiple files into an organized CSV file, so that the output can be verified.
- Exposes an Async based OPCConnectorPool class. It works by creating a subprocess of an implementation of another process.
- Exposes a web endpoint using aiohttp that allows adding nd removing connecxtions dynamically

Make the following request to delete a connector

`curl -H 'Content-Type: application/json' -X POST http://0.0.0.0:8080/remove_connector -d '{"uids":[1]}'`


Make the following request to create a connector

`curl -H 'Content-Type: application/json' -X POST http://0.0.0.0:8080/connector -d '{"connector":[{"url":"opc.tcp://Pranavs-MacBook-Pro-2.local:53530/OPCUA/SimulationServer","node_ids": ["ns=3;i=1001","ns=3;i=1002"], "uid": 1}]}'`


TODOs

- handle connection termination better. Right now, when the keyboardinterrupt is raised sometimes the sub process does not gets a chance for clean shutdown.
- fix the bug that causes the file write to throw an exception, if the control is happening via ConnectorPool