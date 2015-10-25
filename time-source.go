package main

import (
	"flag"
	"time"
	"fmt"
	"github.com/garyburd/redigo/redis"
)

var (
	redisAddress = flag.String("redis-address", ":6379", "Address to the Redis server")
	//interval = flag.Int("interval", 1, "interval in seconds between ticks")
	outputQueue = flag.String("output-queue", "ticktock", "Redis queue to push ticks to")
	maxConnections = flag.Int("max-connections", 10, "Max connections to Redis")
)

func main() {

	flag.Parse()

	redisPool := redis.NewPool(func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", *redisAddress)

		if err != nil {
			return nil, err
		}

		return c, err
	}, *maxConnections)

	defer redisPool.Close()

	queueName := fmt.Sprint("queue.", *outputQueue)
	fmt.Printf("QueueName: %s\n", queueName)

/*
	intervalSeconds := fmt.Sprint(interval, "s")
	fmt.Sprintf("intervalSeconds: %s", intervalSeconds)
	tickDuration, error := time.ParseDuration(intervalSeconds)
	if error != nil {
		fmt.Errorf("Cannot parse duration %s: %s", error, intervalSeconds)
		// todo handle error
	}

	fmt.Sprint("tickduration: %s", tickDuration)*/
	t := time.Tick(1 * time.Second)
	for now := range t {
		tock := fmt.Sprint(now)

		c := redisPool.Get()
		defer c.Close()

		status, err := c.Do("LPUSH", queueName, convertMessage(tock))
		if err != nil {
			fmt.Printf("Cannot LPUSH on queue '%v': %v (%v)\n", queueName, err, status)
		} else {
			fmt.Printf("Pushed '%s' to queue '%s'\n", now, queueName)
		}
	}
}

/*
byte[] newPayload = new byte[((byte[])original.getPayload()).length + headersLength + headerCount * 5 + 2];
		ByteBuffer byteBuffer = ByteBuffer.wrap(newPayload);
		byteBuffer.put((byte) 0xff); // signal new format
		byteBuffer.put((byte) headerCount);
		for (int i = 0; i < headers.length; i++) {
			if (headerValues[i] != null) {
				byteBuffer.put((byte) headers[i].length());
				byteBuffer.put(headers[i].getBytes("UTF-8"));
				byteBuffer.putInt(headerValues[i].length);
				byteBuffer.put(headerValues[i]);
			}
		}
 */


//   format: 0xff, n(1), [ [lenHdr(1), hdr, lenValue(4), value] ... ]
//   sample: "\xff\x01\x0bcontentType\x00\x00\x00\x0c\"text/plain\"2015-10-25 23:13:21"
func convertMessage(payload string) []byte {
	preamble := make([]byte, 3)
	contentTypeHeaderName := []byte("contentType")
	contentTypeHeaderValue := []byte("\"text/plain\"")
	contentTypeHeaderLength := make([]byte, 4)

	// build preamble
	preamble[0] = 0xff // signature
	preamble[1] = 0x01 // number of headers

	// header name length (1-byte)
	preamble[2] = 0x0b // length of "contentType" header (0b == 11)

	// append header name
	preambledHeader := append(preamble, contentTypeHeaderName...)

	// header value length (4-bytes)
	contentTypeHeaderLength[0] = 0x00 // padding
	contentTypeHeaderLength[1] = 0x00 // padding
	contentTypeHeaderLength[2] = 0x00 // padding
	contentTypeHeaderLength[3] = 0x0c // length of header value "text/plain" (0c == 12)

	// append header value
	contentTypeHeader := append(contentTypeHeaderLength, contentTypeHeaderValue...)

	// append message payload
	payloadBytes := []byte(payload)
	serializedPayload := append(contentTypeHeader, payloadBytes...)

	// append serialized headers and payload
	return append(preambledHeader, serializedPayload...);
}
