# Evaluation lab - Akka

## Group number: 10

## Group members

- Student Mu Chaonan 
- Student Cannella Marco
- Student Kirazci Saltuk Bugra 

## Description of message flows
In general, for the RecordMsg, the client sends it to the coordinator. Then the coordinator sends it to one of the workers based on the key.
For the ReportMsg, the client sends it in ask parttern, the coordinator turns to the ReportMode and stashes all the incoming messages. The ReportMsg is forwarded to both workers. Each of worker checks the reset flag. If the reset is TRUE, it sends the value to the coordinator before resetting the variables. If the reset is FALSE, it just sends the value to the coordinator directly. When the coordinator have collected all the replies from both workers, it sends back the result to the client. 
