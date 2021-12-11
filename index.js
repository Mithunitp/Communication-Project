const mqtt = require('mqtt')
const { MongoClient } = require('mongodb')

require('dotenv').config()
const db_user = process.env.DB_USER
const db_password = process.env.DB_PASSWORD
const db_url = process.env.DB_URL
const accountSid = process.env.TWILIO_SID
const authToken = process.env.TWILIO_AUTHTOKEN

const uri = `mongodb+srv://${db_user}:${db_password}@${db_url}`
const twilioClient = require('twilio')(accountSid, authToken)

let mongoClient = new MongoClient(uri)

const storeData = (data) => {
  mongoClient.connect(err => {
    if (err) {
      console.log(`Error: ${err}`)
    }
    const db = mongoClient.db("plant_data")
    db.listCollections({ name: 'sensors' }).next((err, collInfo) => {
      if (err) {
        console.log(`Error: ${error}`)
        process.exit()
      }
      if (!collInfo) {
        db.createCollection("sensors", {
          timeseries: {
            timeField: "ts",
            metaField: "metadata",
            granularity: "seconds"
          }
        }).catch(err => {
          console.log(err)
        })
      }
    })

    let time = Date.now()

    db.collection('sensors').insertMany([{
      "metadata": {
        "type": "temperature",
      },
      "ts": time,
      "value": data.temperature
    },
    {
      "metadata": {
        "type": "humidity",
      },
      "ts": time,
      "value": data.humidity
    }, {
      "metadata": {
        "type": "cloudcover"
      },
      "ts": time,
      "value": data.cloudcover
    }])
  })

}

const notify = (data) => {
  twilioClient.messages.create({
    from: "+18123890146",
    to: "+919414182712",
    body: `Temperature: ${data.temperature}\tHumidity: ${data.humidity}\tCloudcover: ${data.cloudcover}\nMessage: ${data.message}\n`
  }).then(msg => console.log(msg))
    .catch(err => console.error(err))
}

const client = mqtt.connect("mqtt://ec2-52-66-228-90.ap-south-1.compute.amazonaws.com:1883")

client.on("connect", () => {
  client.subscribe("Plant_Monitor", () => {
    console.log("Subscribed to topic: Plant_Monitor")
  })

  client.subscribe("Notifier_Service", () => {
    console.log("Subscribed to topic: Notifier_Service")
  })

  client.on("message", (topic, message) => {
    let data = JSON.parse(message.toString())
    if (topic == "Plant_Monitor") {
      storeData(data)
      console.log(`Temperature: ${data.temperature}\tHumidity: ${data.humidity}\tCloudcover: ${data.cloudcover}\n`)
    } else if (topic == "Notifier_Service") {
      notify(data)
    }
  })
})


