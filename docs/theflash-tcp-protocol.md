# TheFlash TCP Protocol


* Encoding base:
```
Number: use big endian encoding
Flag: [Int64]
Size: [Int64]
String: [Int64][Data]
BinData: [Data] (size is included in the encoded data)
```

* Process
```
Client -> Server
  [Flag:Header]                       - value: 0
    [Int64:ProtocolVersionMajor]      - value: 1
    [Int64:ProtocolVersionMinor]      - value: 1
    [String:ClientName]               - just for debug and log
    [String:DefaultDatabase]          - can be empty
    [String:User]                     - can be empty, means 'default'
    [String:Password]                 - can be empty
    [String:EncoderName]              - only support 'Arrow'
    [Int64:EncoderVersion]            - value: 1
    [Int64:EncoderConcurrentCount]    - value: 1 ~ n

  [Flag:Header]                       - value: 0
    [String:QueryId]
    [String:QueryString]
  ..loop..

Server -> Client
  [Flag:Error]                       - value: 8, any package can be Error
    [String:ErrorString]

  [Flag:Schema]                      - value: 10
    [BinData:EncodedSchema]
  [Flag:Block]                       - value: 11
    [BinData:EncodedBlock]
  ..loop..
  [Flag:End]                         - value: 11
    [Int64:0]
```
