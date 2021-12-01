package main

import (
	"encoding/json"
	"fmt"
        "log"
	"time"

        zmq "github.com/pebbe/zmq4"
	"github.com/influxdata/influxdb-client-go"
	"github.com/spf13/viper"
)

func main() {
	viper.SetConfigName("coha") // name of config file (without extension)
	viper.SetConfigType("yaml") // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath("/etc/coha/")   // path to look for the config file in
	viper.AddConfigPath("$HOME/.coha")  // call multiple times to add many search paths
	viper.AddConfigPath(".")               // optionally look for config in the working directory
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil { // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %w \n", err))
	}

	zctx, _ := zmq.NewContext()
        s, _ := zctx.NewSocket(zmq.SUB)
        s.Connect(viper.GetString("ZmqSocket"))
	s.SetSubscribe("")

	strInfluxUrl := viper.GetString("InfluxUrl")
	strInfluxToken := viper.GetString("InfluxToken")
	tInfluxClient := influxdb2.NewClient(strInfluxUrl, strInfluxToken)
	/* Always close client at the end. */
	defer tInfluxClient.Close()
	/* Create a writer. */
	strInfluxOrganisation := viper.GetString("InfluxOrganisation")
	strInfluxBucket := viper.GetString("InfluxBucket")
	tInfluxWriteAPI := tInfluxClient.WriteAPI(strInfluxOrganisation, strInfluxBucket)

	/* This structure describes only the "id" and "type" fields.
	 * The "id" is used to filter the messages. The "type" is used to
	 * determine what kind of message this is.
	 */
	type MessageGeneric struct {
		Id string `json:"id"`
		Type string `json:"type"`
	}
	type MessageOctetString struct {
		Id string `json:"id"`
		Type string `json:"type"`
		Value string `json:"value"`
	}
	type MessageBoolean struct {
		Id string `json:"id"`
		Type string `json:"type"`
		Value bool `json:"value"`
	}
	type MessageNumber struct {
		Id string `json:"id"`
		Type string `json:"type"`
		Value float64 `json:"value"`
		Precision int `json:"precision"`
		Unit string `json:"unit"`
	}

	/* This is a list of the messages which should be delivered to the
	 * database.
	 */
	astrFilterIds := viper.GetStringSlice("FilterPass")

	for {
		/* Receive one message. */
		msg, err := s.RecvBytes(0)
    		if err != nil {
			log.Printf("Failed to receive a message: %s\n", err)
		} else {
			/* Try to parse the message as JSON. */
			var messages []json.RawMessage

			err := json.Unmarshal(msg, &messages)
			if err != nil {
				log.Printf("Failed to decode the received message: %s\n", err)
			} else {

				for _, item := range messages {
					msg_generic := new(MessageGeneric)
					err := json.Unmarshal(item, &msg_generic)
					if err != nil {
						log.Printf("Failed to get the type field from the message item: %s\n", err)
					} else {
						log.Printf("Id: %s\n", msg_generic.Id)
						bFound := false
						strId := msg_generic.Id
						for _, strFilter := range astrFilterIds {
							if strFilter==strId {
								bFound = true
								break
							}
						}
						if bFound==false {
							log.Printf("Filtering out ID %s\n", strId)
						} else {
							strType := msg_generic.Type
							log.Printf("Type: %s\n", strType)
							var msg_typed interface{}
							switch strType {
							case "octetstring":
								msg_typed = new(MessageOctetString)
							case "boolean":
								msg_typed = new(MessageBoolean)
							case "number":
								msg_typed = new(MessageNumber)
							}
							if msg_typed == nil {
								log.Printf("Unknown field type: %s\n", strType)
							} else {
								err = json.Unmarshal(item, &msg_typed)
								if err != nil {
									log.Printf("Failed to decode the typed message: %s", err)
								} else {
									log.Printf("%#v", msg_typed)

									p := influxdb2.NewPointWithMeasurement("power")
									switch strType {
									case "octetstring":
										p.AddField(strId, msg_typed.(*MessageOctetString).Value)

									case "boolean":
										p.AddField(strId, msg_typed.(*MessageBoolean).Value)

									case "number":
										p.AddTag("unit", msg_typed.(*MessageNumber).Unit)
										p.AddField(strId, msg_typed.(*MessageNumber).Value)
									}
									p.SetTime(time.Now())
									/* write point asynchronously. */
									tInfluxWriteAPI.WritePoint(p)
								}
							}
						}
					}
				}

				/* Flush writes. */
				tInfluxWriteAPI.Flush()
			}
		}
	}
}
