const amqp10 = require('amqp10'),
    AMQPClient = amqp10.Client,
    Policy = amqp10.Policy,
    CONSUMER_GROUP = '$Default',
    PARTITIONS = 4

const ENTITY_PATH = process.env.ENTITY_PATH // enventhub namespace
const AMQP_URL = process.env.AMQP_URL

if (!ENTITY_PATH || !AMQP_URL) {
    console.warn ("ENTITY_PATH and AMQP_URL need to be set");
    process.exit(1);
}

/* This is necessary because EH seems to not be purging messages correctly, so we're date-bounding receiver
const RXFILTER = {
            attach: { source: { filter: {
              'apache.org:selector-filter:string': amqp10.translator(
                ['described', ['symbol', 'apache.org:selector-filter:string'], ['string', `amqp.annotation.x-opt-enqueuedtimeutc > '${new Date().getTime()}'`]])
            } } }
          }
*/
/* receiver links start in "auto-settle" mode, which means that the sender side can consider the message "settled" as soon as it's sent
    Policy.merge(<overrides>, <base policy>)).

    'RenewOnSettle' helper, first number is the initial credit, and the second is the threshold , this:
        Sets the Link's creditQuantum to = initial credit
        Sets the Link to not auto-settle messages at the sender
    now you need to call : link.accept(message) will tell the sender that you've accepted and processed the message.
*/
//var client = new AMQPClient(Policy.EventHub); // Uses PolicyBase default policy
var client = new AMQPClient(Policy.Utils.RenewOnSettle(5, 1, Policy.EventHub))

var received = 0, receivederr = 0
client.connect(AMQP_URL).then(() => {

    for (let i = 0; i < PARTITIONS; i++) {
   //   client.createReceiver(`${ENTITY_PATH}/ConsumerGroups/${CONSUMER_GROUP}/Partitions/${i}`, RXFILTER).then((receiver) => { 
        client.createReceiver(`${ENTITY_PATH}/ConsumerGroups/${CONSUMER_GROUP}/Partitions/${i}`).then((receiver) => {     
            /*
            receiver.on('message', (msg) => {
                received++
                //console.warn(`==> RX ${JSON.stringify(msg)}`)
            })
            */
            receiver.on('errorReceived', (rx_err) => {
                receivederr++
                console.warn(`==> RX ERROR: ${JSON.stringify(rx_err)}`)
            })
        })
    }
})

var r = 0, re = 0
setInterval(() => {
    if (received !== r || receivederr !== re) {
        r = received; re = receivederr;
        console.log (`received : ${received}, errs: ${receivederr}`)
    }
}, 2000);
