const { Kafka } = require('kafkajs')
const { SchemaRegistry, SchemaType } = require('@kafkajs/confluent-schema-registry')

const registry = new SchemaRegistry({ host: 'http://100.26.16.4:8081' })

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
const kafka = new Kafka({
  clientId: 'testing-kafkaja',
  brokers: ['100.26.16.4:9092'],
})

const producer = kafka.producer()


async function main() {
  const { id } = await registry.register(
    { type: SchemaType.JSON, schema },
    { subject: 'mdml-example-kafkajs-value' }
  )
  console.log(id)
  console.log(Date.now())
  await producer.connect()
  const outgoingMessage = {
    value: await registry.encode(id, {
      time: Date.now(),
      description_string: 'Hello, World!',
      some_value: 12.345
    })
  }
  
  await producer.send({
    topic: "mdml-example-kafkajs",
    messages: [ outgoingMessage ]
  })
  
  await producer.disconnect()

}

main()
