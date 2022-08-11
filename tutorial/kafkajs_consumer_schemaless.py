var { Kafka } = require('kafkajs')

var kafka = new Kafka({
  clientId: 'testing-kafkaja',
  brokers: ['100.26.16.4:9092'],
})

async function main() {

  var consumer = kafka.consumer({ groupId: 'test' })

  await consumer.connect()
  await consumer.subscribe({ topics: ['mdml-kafkajs-test-topic'], fromBeginning: false })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      dat = JSON.parse(message.value)
      console.log("Message: " + JSON.stringify(dat));
      console.log("Message string part: " + dat.string);
      console.log("Message number part: " + dat.number);
    },
  })
}

main();
