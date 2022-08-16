const { Kafka } = require('kafkajs')
const { SchemaRegistry, SchemaType } = require('@kafkajs/confluent-schema-registry')

const registry = new SchemaRegistry({ host: 'http://100.26.16.4:8081' })
const kafka = new Kafka({
  brokers: ['100.26.16.4:9092'],
  // clientId: 'example-consumer',
})
const consumer = kafka.consumer({ groupId: 'test-group' })

const topic = 'mdml-example-kafkajs'

const run = async () => {

  const schema = `
  {
    "definitions" : {
      "record:examples.Test" : {
        "type" : "object",
        "required" : [ "description_string", "some_value" ],
        "additionalProperties" : false,
        "properties" : {
          "time" : {
            "type": "number"
          },
          "description_string" : {
            "type": "string"
          },
          "some_value" : {
            "type" : "number"
          }
        }
      }
    },
    "$ref" : "#/definitions/record:examples.Test"
    }
  `
  const { id } = await registry.register(
    { type: SchemaType.JSON, schema },
    { subject: 'mdml-example-kafkajs-value' }
  )
  await consumer.connect()

  await consumer.subscribe({ topic: topic })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const decodedMessage = {
        ...message,
        value: await registry.decode(message.value)
      }
      console.log(decodedMessage)
    },
  })
}

run().catch(async e => {
  console.error(e)
  consumer && await consumer.disconnect()
  process.exit(1)
})

