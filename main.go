package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/creepfmd/jsonpath"
	"github.com/go-redis/redis"
	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	elastic "gopkg.in/olivere/elastic.v3"
)

type analyticsObject struct {
	Timestamp int64                  `json:"timestamp"`
	Filters   map[string]string      `json:"filters"`
	Values    map[string]interface{} `json:"values"`
}

type analyzerConfig struct {
	IDField   string   `bson:"idField"`
	TypeField string   `bson:"typeField"`
	TimeField string   `bson:"timeField"`
	Filters   []string `bson:"filters"`
	Values    []string `bson:"values"`
}

type destinationConfig struct {
	SystemID        string        `bson:"systemId"`
	SystemName      string        `bson:"systemName"`
	RouteCondition  string        `bson:"routeCondition"`
	Split           string        `bson:"split"`
	AfterloadScript string        `bson:"afterloadScript"`
	PreloadScript   string        `bson:"preloadScript"`
	PreloadActions  []interface{} `bson:"preloadActions"`
}

type objectConfig struct {
	ObjectID       string         `bson:"objectId"`
	Analyzer       analyzerConfig `bson:"analyzer"`
	PreloadActions []struct {
		ActionID         string   `bson:"actionId"`
		ActionParameters []string `bson:"actionParameters"`
	} `bson:"preloadActions"`
	Destinations []destinationConfig `bson:"destinations"`
}

type systemConfig struct {
	PublishToken string         `bson:"publishToken"`
	SystemID     string         `bson:"systemId"`
	UserID       string         `bson:"userId"`
	ObjectTypes  []objectConfig `bson:"objectTypes"`
	SystemName   string         `bson:"systemName"`
}

func main() {
	if os.Getenv("LOGGER_URL") == "" {
		os.Setenv("LOGGER_URL", "http://logger:8084/")
	}
	if os.Getenv("SPLITTER_URL") == "" {
		os.Setenv("SPLITTER_URL", "http://localhost:8082/")
	}
	if os.Getenv("SYSTEM_ID") == "" {
		os.Setenv("SYSTEM_ID", "2dda8512f170683e224f17ebb2295aaf")
	}
	if os.Getenv("MONGO_URL") == "" {
		os.Setenv("MONGO_URL", "docker.rutt.io")
	}
	if os.Getenv("AMQP_URL") == "" {
		os.Setenv("AMQP_URL", "amqp://mqadmin:mqadmin@dev-rabbit-1.rutt.io//")
	}
	if os.Getenv("REDIS_URL") == "" {
		os.Setenv("REDIS_URL", "docker.rutt.io:6379")
	}
	if os.Getenv("ELASTICSEARCH_URL") == "" {
		os.Setenv("ELASTICSEARCH_URL", "http://dev-elastic.rutt.io:9200")
	}

	log.Println("Connecting redis at " + os.Getenv("REDIS_URL"))
	red := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_URL"),
		Password: "",
		DB:       0,
	})
	_, err := red.Ping().Result()
	failOnError(err, "Failed to connect to redis")

	log.Println("Connecting mongo at " + os.Getenv("MONGO_URL"))
	session, err := mgo.Dial(os.Getenv("MONGO_URL"))
	failOnError(err, "Failed to connect to mongo")
	defer session.Close()

	log.Println("Connecting rabbit at " + os.Getenv("AMQP_URL"))
	conn, err := amqp.Dial(os.Getenv("AMQP_URL"))
	failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	defer conn.Close()

	log.Println("Connecting elasticsearch at " + os.Getenv("ELASTICSEARCH_URL"))
	elasticClient, err := elastic.NewClient(elastic.SetURL(os.Getenv("ELASTICSEARCH_URL")))
	failOnError(err, "Failed to connect to Elasticsearch")

	log.Println("Setting collection...")
	c := session.DB("rutt").C("systems")
	objs := getSystemConfig(c, elasticClient)

	// launch consumer
	msgs, err := ch.Consume(
		os.Getenv("SYSTEM_ID"), // queue
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			res := consumeMessage(objs, d, elasticClient, red)
			progressOnError(res, "Found nothing")
		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func consumeMessage(objects systemConfig, d amqp.Delivery, elasticClient *elastic.Client, redisClient *redis.Client) error {
	log.Printf("Received a message: %s", d.CorrelationId)
	foundSomething := false
	var err error

	for _, v := range objects.ObjectTypes {
		if v.ObjectID == d.RoutingKey {
			foundSomething = true
			var jsonData interface{}
			err = json.Unmarshal(d.Body, &jsonData)
			if err != nil {
				progressOnError(err, "Not JSON body")
				return err
			}

			// analyzer goes here if got "analyzer" field
			/*
				if len(v.Analyzer.Values) > 0 {
					var obj analyticsObject
					obj.Filters = make(map[string]string)
					obj.Values = make(map[string]interface{})
					analyticsID := d.CorrelationId
					analyticsType := d.RoutingKey
					obj.Timestamp = time.Now().UnixNano() / 1000000

					if v.Analyzer.IDField != "" {
						jpath, _ := jsonpath.Compile("$." + v.Analyzer.IDField)
						res, _ := jpath.Lookup(jsonData)
						analyticsID = res.(string)
					}

					if v.Analyzer.TimeField != "" {
						jpath, _ := jsonpath.Compile("$." + v.Analyzer.TimeField)
						res, _ := jpath.Lookup(jsonData)
						obj.Timestamp = int64(res.(float64))
					}

					for _, k := range v.Analyzer.Filters {
						jpath, _ := jsonpath.Compile("$." + k)
						res, _ := jpath.Lookup(jsonData)
						obj.Filters[k] = res.(string)
					}

					for _, k := range v.Analyzer.Values {
						jpath, _ := jsonpath.Compile("$." + k)
						res, _ := jpath.Lookup(jsonData)
						switch res.(type) {
						case float64:
							obj.Values[k] = res.(float64)
						case string:
							obj.Values[k] = res.(string)
						}

					}
					objJSON, err := json.Marshal(&obj)
					failOnError(err, "Failed to make analytics message")

					_, err = elasticClient.Index().
						Index(objects.UserID).
						Type(analyticsType).
						Id(analyticsID).
						BodyJson(string(objJSON)).
						Do()
					progressOnError(err, "Failed to index analytics")
				}
				// ends here
			*/
			// preload actions goes here
			// should modify jsonString
			for _, action := range v.PreloadActions {
				log.Println(action)
			}
			// ends here

			publishToDestination(redisClient, d, jsonData, v)
		}
	}

	if !foundSomething {
		log.Println("Routing key not matching any object")
		err = d.Ack(false)
		progressOnError(err, "Failed to ack")
	}
	return err
}

func publishToDestination(r *redis.Client, d amqp.Delivery, jsonData interface{}, objectType objectConfig) {
	destTotal := len(objectType.Destinations)
	destDone := 0
	pipe := r.TxPipeline()
	for _, dest := range objectType.Destinations {
		shouldBeRouted := true
		if len(dest.RouteCondition) > 0 {
			jpath, _ := jsonpath.Compile("$." + dest.RouteCondition)
			_, err := jpath.Lookup(jsonData)
			if err == nil {
				shouldBeRouted = false
			}
		}
		if shouldBeRouted {
			// split message into parts

			resp, err := http.Post(os.Getenv("SPLITTER_URL")+dest.Split, "application/json", bytes.NewBuffer(d.Body))
			progressOnError(err, "[SPLITTER] error splitting")
			defer resp.Body.Close()
			body, _ := ioutil.ReadAll(resp.Body)
			bodyString := string(body)

			// if bodyString is json array, then iterate
			// else send original message
			var u1, u4, u5 uuid.UUID
			u1, err = uuid.NewV1()
			u4, err = uuid.NewV4()
			u5 = uuid.NewV5(u4, u1.String())
			failOnError(err, "Failed generating UUID")
			pipe.HMSet(u5.String(), map[string]interface{}{
				"message":       bodyString,
				"correlationId": d.CorrelationId,
				"queue":         dest.SystemID + ".outgoing",
				"publishTime":   d.Timestamp.UnixNano() / 1000000,
			})
			pipe.ZAdd(dest.SystemID+".outgoing", redis.Z{Score: float64(d.Timestamp.UnixNano() / 1000000), Member: u5.String()})
		}
		//check all destinations done
		destDone++
		if destDone == destTotal {
			_, err := pipe.Exec()
			if err == nil {
				log.Println("[ACKING] " + d.CorrelationId)
				d.Ack(false)
				pipe = nil
			} else {
				progressOnError(err, "[REDIS] publish error")
				pipe.Discard()
				pipe = nil
			}
		}

	}
}

func getSystemConfig(c *mgo.Collection, elasticClient *elastic.Client) systemConfig {
	var result systemConfig
	var user map[string]string

	err := c.Find(bson.M{"systemId": os.Getenv("SYSTEM_ID")}).Select(bson.M{"_id": 0, "userId": 1}).One(&user)
	failOnError(err, "Failed to get config")
	userID := user["userId"]

	err = c.Find(bson.M{"systemId": os.Getenv("SYSTEM_ID")}).Select(bson.M{"_id": 0}).One(&result)
	failOnError(err, "Failed to get config")

	var indexExists bool
	indexExists, err = elasticClient.IndexExists(userID).Do()
	failOnError(err, "Failed to check user index")
	if !indexExists {
		_, err = elasticClient.CreateIndex(userID).Do()
		failOnError(err, "Failed to create user index")
	}

	// for each object upsert mapping
	for _, v := range result.ObjectTypes {
		_, err = elasticClient.PutMapping().
			Index(userID).
			Type(v.ObjectID).
			BodyString(`{
		               "properties": {
		                 "timestamp": {
		                   "type": "date"
		                 }
		               },
		               "dynamic_templates": [
		                 {
		                   "notanalyzed": {
		                     "match": "*",
		                     "match_mapping_type": "string",
		                     "mapping": {
		                       "type": "string",
		                       "index": "not_analyzed"
		                     }
		                   }
		                 }
		               ]
		             }
		           }`).
			Do()
		failOnError(err, "Failed to create mapping")
	}

	return result
}

func logDirectorStart(correlationID string) {
	currentTime := time.Now().UnixNano() / 1000000
	_, err := http.Get(os.Getenv("LOGGER_URL") + "update/" + correlationID + "/timeDirectorStart/" + (string)(currentTime))
	progressOnError(err, "Failed logging new message")
}

func logDestinationAdded(correlationID string, systemID string, newMessageUID string) {
	currentTime := time.Now().UnixNano() / 1000000
	_, err := http.Get(os.Getenv("LOGGER_URL") + "destinationAdded/" + correlationID + "/" + systemID + "/" + newMessageUID + "/" + (string)(currentTime))
	progressOnError(err, "Failed logging new message")
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func progressOnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}
