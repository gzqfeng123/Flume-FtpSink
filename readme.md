## Flume Ftp Sink

| Property Name	 | Default | Description |
| :------------: |:------------| :-----|
| type | - | need to be `com.mat.flume.sink.FtpSink` |
| host | - | host |
| port | 21| port |
| user | -| user |
| password | -| password |
| ftpDirPath | /| ftp upload dir |
| prefix | DATA-| file prefix | 
| suffix | .dat| file prefix |
| tempSuffix | .tmp | temp suffix when uploading |
| batchSize | 1000 | max records of a file |
