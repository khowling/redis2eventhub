const Redis = require('ioredis');

const cluster = require('cluster');
const numCPUs = require('os').cpus().length;

const REDIS_CHANNEL = process.env.REDIS_CHANNEL || 'clickpath' // redis queue

if (cluster.isMaster) {
    // Fork workers.
    for (var i = 0; i < (process.env.WORKERS || numCPUs); i++) {
        cluster.fork();
    }
    cluster.on('exit', (worker, code, signal) => {
        console.log(`worker ${worker.process.pid} exit`);
    });
} else {
    console.log(`worker started #${cluster.worker.id}`);
    

    const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379/3');
    redis.on("error", (c) => console.log (`redis error: ${JSON.stringify(c)}`));
    redis.on("ready", (c) => {
        console.log ('redis ready');
        let promiseArray = [];
        for (var l = 0; l < (process.env.MESSAGES || 1000); l++) {
            let jsonmsg_json = `{"itteration":"${l}","workerid":"${cluster.worker.id}","message":"hello from ${cluster.worker.id}:${l}"}`,
                jsonmsg_obj = JSON.parse(jsonmsg_json);
            let publish_promise = 
 
            promiseArray.push (() => {
                return new Promise((a,b) => 
                    setTimeout (() => {
                        redis.publish(REDIS_CHANNEL,  jsonmsg_json)
                        a()
                    }, 1))});
        }
        promiseArray.reduce((p, fn) => p.then(fn, (err) => console.log (`send error : ${JSON.stringify(err)}`)), Promise.resolve()).then((res) => {
            console.log (`finished ${cluster.worker.id}`);
            //redis.quit();
            process.exit(0);
        })
    });
}