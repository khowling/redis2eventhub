// push demo data as fast as possible directly to eventhub.


const amqp10 = require('amqp10'),
    AMQPClient = amqp10.Client,
    Policy = amqp10.Policy

const ENTITY_PATH = process.env.ENTITY_PATH // enventhub namespace
const AMQP_URL = process.env.AMQP_URL

if (!ENTITY_PATH || !AMQP_URL) {
    console.warn ("ENTITY_PATH and AMQP_URL need to be set");
    process.exit(1);
}

var client = new AMQPClient(Policy.EventHub) // Uses PolicyBase default policy
var sent = 0, settled = 0, errors = 0, linkCredit = 0, waiting_pop = false
// create AMQP Connection to the Azure Service Bus Container namespace (expensive, setup TCP/TLS etc)
client.connect(AMQP_URL).then(() => {
    console.log ('connected')
    // this nodejs app 'Node', opens up a 'link' by calling 'attach' between itself (as sender) & the EventHub 'Node' (as receiver)
    client.createSender(ENTITY_PATH).then ((sender) => {
        console.log ('sender created')
        sender.on('errorReceived', (tx_err) => {
            console.warn('===> TX ERROR: ', tx_err)
            console.log(`===> TX messages sending: ${sent - (settled+errors)}, settled: ${settled}, failed: ${errors} (linkCredit: ${linkCredit})`)
        });

        const subscribe = () => {
            let bytes = parseInt(Math.random().toString().substr(2,1 + Math.floor(Math.random() * 6))),
                site = ["youtube", "youtube", "netflix", "netflix", "netflix", "iplayer", "other"][Math.floor(Math.random() * 7)],
                message = `{"url" : "https://${site}/wfwefwef", "time": ${new Date().getTime()}, "site": "${site}", "bytes": ${bytes}}`

            try {
                let msg = JSON.parse(message)
                sender.send(msg).then ((res) => {
                    settled++; 
                }, (err) => {
                    errors++;
                    console.log (`===> TX Error: ${JSON.stringify(err)}`)
                    // most likly throttling, so wait
                    if (sint) {
                        console.log('PAUSING, Errors MOST LIKLEY THROTTLING')
                        clearInterval (sint); sint = null;
                        setTimeout(() => { 
                            console.log ('restarting..')
                            sint = setInterval(subscribe, Math.random() * DELAY_TO_PREVENT_THROTTLING) }, 15000)
                    }
                });
                sent++;
            } catch (e) {
                errors++;
                console.error (`failed to JSON parse  message ${e}`)
            }
        };

        const DELAY_TO_PREVENT_THROTTLING = 5 // 5 should be ok for 1 unit
        let sint = null
        // Manage message flow control
        sender.on('creditChange', (flow) => {
            //console.log(`===> TX flow frame: linkCredit: ${flow.linkCredit}, delivery: ${flow.deliveryCount}. messages sending: ${sent - (settled+errors)}, settled: ${settled}, failed: ${errors} (linkCredit: ${linkCredit})`)
            linkCredit = flow.linkCredit
            if (sint && (sent - (settled+errors) > (linkCredit - 50))) {
                console.log('PAUSING, LINK ALMOST FULL')
                clearInterval (sint); sint = null;
                setTimeout(() => { 
                    console.log ('restarting..')
                    sint = setInterval(subscribe, Math.random() * DELAY_TO_PREVENT_THROTTLING) }, 15000)
            }
        })

        sint = setInterval(subscribe, Math.random() * DELAY_TO_PREVENT_THROTTLING)


        // close down connections on ctrl-c
        process.on ('SIGINT', (code) => {
            try {
                sender.detach().then((res) => console.log ('detached sender'))
                console.log ('closed connetions')
            } catch (e) {
                console.log (`cannot detach sender: ${e}`)
            }
            setInterval (() => process.exit(0), 500);
        })
    }, (err) => console.log (`failed to create sneder : ${err}`))
}, (err) => console.log (`failed to connect to eventhub : ${err}`))


// stdout logging
var s = 0, c = 0, e = 0 // to detect changes in metrics, only log if changed
setInterval(() => {
    if (sent !== s || settled !== c || errors !== e) {
        s = sent; c = settled; e = errors;
        console.log (`messages sending: ${sent - (settled+errors)}, settled: ${settled}, failed: ${errors} (linkCredit: ${linkCredit} / waiting: ${waiting_pop})`)
    }
}, 2000);

