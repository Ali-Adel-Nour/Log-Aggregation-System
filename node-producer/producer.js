const amqp = require('amqplib');

let connection = null;
let channel = null;

const levels = ['INFO', 'ERROR', 'DEBUG'];
const randomLevel = levels[Math.floor(Math.random() * levels.length)];

async function setupConnection() {
    connection = await amqp.connect('amqp://localhost');
    channel = await connection.createChannel();
    const queue = 'logs';

    await channel.assertQueue(queue, {
        durable: true
    });
    return queue;
}

async function sendLog() {
    try {
        if (!channel) {
            await setupConnection();
        }

        const queue = 'logs';
        const log = JSON.stringify({
                timestamp: new Date().toISOString(),
                level: 'INFO',
                message: 'User logged in',
                service: 'auth-service',
                user_id: '12345',
                ip_address: '192.168.1.1',
                endpoint: '/login',
              });

        channel.sendToQueue(queue, Buffer.from(log), {
            persistent: true // Make messages persistent
        });
        console.log('Log sent:', log);
    } catch (error) {
        console.error('Error sending log:', error);
        if (channel) await channel.close();
        if (connection) await connection.close();
        channel = null;
        connection = null;
    }
}


process.on('SIGINT', async () => {
    if (channel) await channel.close();
    if (connection) await connection.close();
    process.exit(0);
});


setInterval(sendLog, 1000);