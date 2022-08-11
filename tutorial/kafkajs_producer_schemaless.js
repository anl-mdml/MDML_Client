const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'testing-kafkaja',
  brokers: ['100.26.16.4:9092'],
})

const producer = kafka.producer()

async function main() {
  await producer.connect()
  
  var someMessage = {
    string: "Hello world",
    number: 12.345
  }

  await producer.send({
    topic: 'mdml-kafkajs-test-topic',
    messages: [
      { value: JSON.stringify(someMessage)},
    ],
  })
  
  await producer.disconnect()

}

main()
