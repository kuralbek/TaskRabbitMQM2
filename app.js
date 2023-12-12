const amqp = require('amqplib')
const winston = require('winston')
const rabbitURL = 'amqp://localhost'

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.json(),
    transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: 'logfile_m2.log' })
    ]
});

const proccesTask = async (task) => {

    try {
        logger.info(`Начало обработки задания: ${task}`);
        await  new Promise(resolve => setTimeout(resolve,5000))
        const result =  task * 2
        logger.info(`Завершение обработки задания: ${task}. Результат: ${ result}`);
        return result
    }catch (e) {
        logger.error(`Ошибка при обработке задания: ${task}. Ошибка: ${e.message}`);
    }

}

const startWorker = async ()=>{
    try {
        const connect = await amqp.connect(rabbitURL)
        const chanel = await connect.createChannel()

        await chanel.assertQueue('testQu',{durable:false})

        chanel.consume('testQu', async (messa) => {
            try {
                const task = JSON.parse(messa.content.toString())


                if (messa){
                    const content = JSON.parse(messa.content.toString())

                    const replayTo = content.replyTo

                    const message = content.task.message

                    logger.info(`Начало обработки задания: ${message}`);

                    const result = await proccesTask(message)

                    logger.info(`Завершение обработки задания: ${message}. Результат: ${result}`);


                    chanel.sendToQueue(replayTo,Buffer.from(JSON.stringify(result)))

                }
            }catch (e) {
                logger.error(`Ошибка при обработке задания: ${e.message}`);

            }
        },{ noAck: true })

    }catch (e) {
        logger.error(`Ошибка при подключении: ${e.message}`);
    }

}

startWorker()