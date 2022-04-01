// import the `Kafka` instance from the kafkajs library
const { Kafka } = require("kafkajs");

// the client ID lets kafka know who's producing the messages
const clientId = "my-app";
// we can define the list of brokers in the cluster
const brokers = ["localhost:9092", "localhost:9094", "localhost:9095"];
// this is the topic to which we want to write messages
const topic = "message-log";

// initialize a new kafka client and initialize a producer from it
const kafka = new Kafka({ clientId, brokers });
const producer = kafka.producer();

// we define an async function that writes a new message each second
const produce = async () => {
  await producer.connect();
  let i = 0;
  let j = 0;

  // after the produce has connected, we start an interval timer
  setInterval(async () => {
    try {
      // send a message to the configured topic with
      // the key and value formed from the current value of `i`
      await producer.send({
        topic,
        //acks: 1,
        key: 'haha',
        messages: [
          {
            key: String(i),
            value: "this is message " + i,
          },
         
        ],
      });

      // if the message is written successfully, log it and increment `i`
      console.log("writes: ", i);
      i++;
    } catch (err) {
      console.error("could not write message " + err);
    }
  }, 1000);

  setInterval(async () => {
    try {
      // send a message to the configured topic with
      // the key and value formed from the current value of `i`
      await producer.send({
        topic,
        //acks: 1,
        key: 'kaka',
        messages: [
          {
            key: String(j),
            value: "Thaiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii " + j,
          },
         
        ],
      });

      // if the message is written successfully, log it and increment `i`
      console.log("writes kakakaka: ", j, "--------------------");
      j++;
    } catch (err) {
      console.error("could not write message " + err);
    }
  }, 1000);
};

module.exports = produce;
