# OrbcommIngester

```mermaid
graph LR
O[ORBCOMM S-AIS]
I(Ingester)
T((Timestream))

O --- |SSL Websocket| I
I --> T
```

## Running Locally

🚨 **We only pay for 1 Orbcomm Stream reader, so if you run locally it cuts off our main reader in AWS.** 🚨

If your local stream is getting no messages then it is likely another service is connected to the stream.

### Via Docker [RECOMMENDED]

ℹ️ Make sure to have docker installed and setup locally first! [Instructions](https://www.docker.com/get-started/)

```sh
docker build -t spotship/orbcomm-ingester:latest .
docker run spotship/orbcomm-ingester:latest
```

### Via Pipenv

```sh
pipenv install
pipenv run python app.py
```

## CICD

Uses Github actions to push to ECR and deploy the service on AWS Fargate. The workflow is in `.github/workflows/aws.yml`

This is started automatically on a successful pr merge into `main` branch.

_N.B._ can be started manually in the github actions console by clicking run workflow and choosing the branch to use.

You can edit the task definition to edit the service configuration in `taskdef.json`.

[Instructions used to setup](https://www.awstutorials.cloud/post/tutorials/ecs-deploy-github-actions/).

Uses `Github` IAM user credentials in repo secret & `orbcomm-ingester`ECR repo.

---

## AIS Message types

### Useful links

- [What is AIS?](https://www.marineinsight.com/marine-navigation/automatic-identification-system-ais-integrating-and-identifying-marine-communication-channels/)
- [Message types](https://arundaleais.github.io/docs/ais/ais_message_types.html)
- [Timestream Write](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/timestream-write.html)

### Type 1

#### Example

```json
{
  "type": 1,
  "repeat": 0,
  "mmsi": "657177000",
  "status": 8,
  "turn": 0,
  "speed": 0.8,
  "accuracy": 1,
  "lon": 7.824271666666666,
  "lat": 4.016156666666666,
  "course": 205.8,
  "heading": 225,
  "second": 14,
  "maneuver": 0,
  "raim": 0,
  "radio": 24812
}
```

### Type 2

#### Example

```json
{
  "type": 1,
  "repeat": 0,
  "mmsi": "657177000",
  "status": 8,
  "turn": 0,
  "speed": 0.8,
  "accuracy": 1,
  "lon": 7.824271666666666,
  "lat": 4.016156666666666,
  "course": 205.8,
  "heading": 225,
  "second": 14,
  "maneuver": 0,
  "raim": 0,
  "radio": 24812
}
```

### Type 3

#### Example

```json
{
    "msg_type": 3,
    "repeat": 0,
    "mmsi": 272700000,
    "status": 5,
    "turn": 0.0,
    "speed": 0.0,
    "accuracy": True,
    "lon": 31.021412,
    "lat": 46.602015,
    "course": 302.6,
    "heading": 68,
    "second": 56,
    "maneuver": 3,
    "spare_1": b"\xc0",
    "raim": True,
    "radio": 58380
}
```

### Type 4

#### Example

```json
{
    "type": 4,
    "repeat": 0,
    "mmsi": "006631000",
    "year": 0,
    "month": 0,
    "day": 0,
    "hour": 24,
    "minute": 60,
    "second": 60,
    "accuracy": 0,
    "lon": 181.0,
    "lat": 91.0,
    "epfd": <EpfdType.Undefined: 0>,
    "raim": 0,
    "radio": 147456
}
```

### Type 5

#### Example

```json
{
    "msg_type": 5,
    "repeat": 0,
    "mmsi": 413236380,
    "ais_version": 0,
    "imo": 0,
    "callsign": "0",
    "shipname": "FENG CHUAN 68",
    "ship_type": 70,
    "to_bow": 45,
    "to_stern": 12,
    "to_port": 6,
    "to_starboard": 4,
    "epfd": 0,
    "month": 12,
    "day": 4,
    "hour": 9,
    "minute": 0,
    "draught": 1.9,
    "destination": "FUZHOU",
    "dte": True
}
```

### Type 6

#### Example

```json
{
  "type": 6,
  "repeat": 0,
  "mmsi": "997011052",
  "seqno": 0,
  "dest_mmsi": "000000000",
  "retransmit": 0,
  "dac": 235,
  "fid": 10,
  "data": "00111111110000000000000010000000000000000000000000"
}
```

### Type 7

#### Example

```json
{
  "type": 7,
  "repeat": 0,
  "mmsi": "803866656",
  "mmsi1": "034078723",
  "mmsiseq1": 0,
  "mmsi2": "000000000",
  "mmsiseq2": 0,
  "mmsi3": "000000000",
  "mmsiseq3": 0,
  "mmsi4": "000000000",
  "mmsiseq4": 0
}
```

### Type 8

#### Example

```json
{
  "type": 8,
  "repeat": 0,
  "mmsi": "545392672",
  "dac": 32,
  "fid": 32,
  "data": "1000000000"
}
```

### Type 9

#### Example

```json
{
  "type": 9,
  "repeat": 0,
  "mmsi": "111277501",
  "alt": 166,
  "speed": 85,
  "accuracy": 1,
  "lon": 21.527515,
  "lat": 55.86421833333333,
  "course": 16.5,
  "second": 63,
  "dte": 1,
  "assigned": 0,
  "raim": 0,
  "radio": 99126
}
```

### Type 10

#### Example

```json
{
  "type": 10,
  "repeat": 0,
  "mmsi": "332044320",
  "dest_mmsi": "034086918"
}
```

### Type 11

#### Example

```json
{
    "type": 11,
    "repeat": 0,
    "mmsi": "366916740",
    "year": 2021,
    "month": 5,
    "day": 5,
    "hour": 16,
    "minute": 8,
    "second": 4,
    "accuracy": 0,
    "lon": -166.21348166666667,
    "lat": 54.15264333333333,
    "epfd": <EpfdType.GPS: 1>,
    "raim": 0,
    "radio": 0
}
```

### Type 12

#### Example

```json
{
  "type": 12,
  "repeat": 0,
  "mmsi": "004310704",
  "seqno": 2,
  "dest_mmsi": "441158000",
  "retransmit": 0,
  "text": "<WARNING> YOU ARE GOING TO AGROUND. PLEASE CHECK"
}
```

### Type 13

#### Example

```json
{
  "type": 13,
  "repeat": 3,
  "mmsi": "092340564",
  "mmsi1": "150077699",
  "mmsiseq1": 0,
  "mmsi2": "000000000",
  "mmsiseq2": 0,
  "mmsi3": "000000000",
  "mmsiseq3": 0,
  "mmsi4": "000000000",
  "mmsiseq4": 0
}
```

### Type 14

#### Example

```json
{
  "type": 14,
  "repeat": 0,
  "mmsi": "503316566",
  "text": "LPE"
}
```

### Type 15

#### Example

```json
{
  "type": 15,
  "repeat": 0,
  "mmsi": "000000000",
  "mmsi1": "000000008",
  "type1_1": 0,
  "offset1_1": 0,
  "type1_2": 0,
  "offset1_2": 0,
  "mmsi2": "000000000",
  "type2_1": 0,
  "offset2_1": 0
}
```

### Type 16

#### Example

```json
{
  "type": 16,
  "repeat": 1,
  "mmsi": "431804640",
  "mmsi1": "021504015",
  "offset1": 0,
  "increment1": 0,
  "mmsi2": "000000000",
  "offset2": 0,
  "increment2": 0
}
```

### Type 17

#### Example

```json
{
  "type": 17,
  "repeat": 0,
  "mmsi": "545392672",
  "lon": 8322,
  "lat": 0,
  "data": 0
}
```

### Type 18

#### Example

```json
{
    "msg_type": 18,
    "repeat": 0,
    "mmsi": 316044687,
    "reserved_1": 0,
    "speed": 0.0,
    "accuracy": True,
    "lon": -122.499012,
    "lat": 37.972215,
    "course": 283.9,
    "heading": 511,
    "second": 20,
    "reserved_2": 0,
    "cs": True,
    "display": False,
    "dsc": True,
    "band": True,
    "msg22": False,
    "assigned": False,
    "raim": True,
    "radio": 917510
}
```

### Type 19

#### Example

```json
{
    "type": 19,
    "repeat": 0,
    "mmsi": "413848311",
    "speed": 0.1,
    "accuracy": 0,
    "lon": 120.20996166666667,
    "lat": 31.94901,
    "course": 68.4,
    "heading": 511,
    "second": 60,
    "regional": 0,
    "shipname": "JINWANG558",
    "shiptype": <ShipType.Cargo: 70>,
    "to_bow": 5,
    "to_stern": 57,
    "to_port": 5,
    "to_starboard": 7,
    "epfd": <EpfdType.Undefined: 0>,
    "raim": 0,
    "dte": 1,
    "assigned": 0
}
```

### Type 20

#### Example

```json
{
  "type": 20,
  "repeat": 0,
  "mmsi": "545392672",
  "offset1": 130,
  "number1": 0,
  "timeout1": 4,
  "increment1": 0,
  "offset2": 0,
  "number2": 0,
  "timeout2": 0,
  "increment2": 0,
  "offset3": 0,
  "number3": 0,
  "timeout3": 0,
  "increment3": 0,
  "offset4": 0,
  "number4": 0,
  "timeout4": 0,
  "increment4": 0
}
```

### Type 21

#### Example

```json
{
    "type": 21,
    "repeat": 0,
    "mmsi": "992471026",
    "aid_type": <NavAid.SPARE: 5>,
    "name": "E1438 MONTE PORO",
    "accuracy": 0,
    "lon": 10.237318333333333,
    "lat": 42.727788333333336,
    "to_bow": 1,
    "to_stern": 1,
    "to_port": 1,
    "to_starboard": 1,
    "epfd": <EpfdType.Undefined: 0>,
    "second": 60,
    "off_position": 0,
    "regional": 0,
    "raim": 0,
    "virtual_aid": 0,
    "assigned": 0,
    "name_extension": ""
}
```

### Type 22

#### Example

```json

```

### Type 23

#### Example

```json
{
    "type": 23,
    "repeat": 0,
    "mmsi": "1048707072",
    "ne_lon": 0.0,
    "ne_lat": 0.0,
    "sw_lon": 0.0,
    "sw_lat": 0.0,
    "station_type": <StationType.ALL: 0>,
    "ship_type": <ShipType.NotAvailable: 0>,
    "txrx": <TransmitMode.TXA_TXB_RXA_RXB: 0>,
    "interval": <StationIntervals.AUTONOMOUS_MODE: 0>,
    "quiet": 0
}
```

### Type 24

#### Example

```json
{
  "type": 24,
  "mmsi": "512039000",
  "shipname": "RV POLARIS II"
}
```

### Type 25

#### Example

```json
{
  "type": 25,
  "repeat": 0,
  "mmsi": "025165824",
  "addressed": 0,
  "structured": 0,
  "data": ""
}
```

### Type 26

#### Example

```json
{
  "type": 26,
  "repeat": 0,
  "mmsi": "367597230",
  "addressed": 0,
  "structured": 1,
  "radio": 584224,
  "app_id": 5768,
  "data": "11000011001010011000011111101111011010100111100100010110011001001100111111100011110101000010111100011101000101011100001000"
}
```

### Type 27

#### Example

```json
{
    "type": 27,
    "repeat": 0,
    "mmsi": "232021787",
    "accuracy": 1,
    "raim": 1,
    "status": <NavigationStatus.Undefined: 15>,
    "lon": -61.85,
    "lat": 17.118333333333332,
    "speed": 0,
    "course": 511,
    "gnss": 0
}
```

---

Markdown linting is enabled by an extension in vscode [markdownlint](https://marketplace.visualstudio.com/items?itemName=DavidAnson.vscode-markdownlint)

These rules can be configured using `.markdownlint.json`

- [Markdown Cheat Sheet](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet)
- [Markdown Emoji Cheatsheet](https://gist.github.com/rxaviers/7360908)
- [Markdown CodeBlock Language List](https://github.com/github/linguist/blob/master/lib/linguist/languages.yml)
- [Mermaid Diagram Visual Editor](https://mermaid.live)
- [Mermaid CheatSheet](https://jojozhuang.github.io/tutorial/mermaid-cheat-sheet/)
