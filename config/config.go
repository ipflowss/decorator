package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type Configuration struct {
	BrokerList        []string `json:"brokerList, omitempty"`
	*ConsumerTopic     string   `json:"consumerTopic, omitempty"`
	ProducerTopic     string   `json:"producerTopic, omitempty"`
	Partition         int      `json:"partition"`
	OffsetType        int      `json:"offsetType"`
	MessageCountStart int      `json:"messageCountStart"`
}

func NewConfigHandle() Configuration {
	return Configuration{}
}

func (c Configuration) ParseConfig(filename *string) Configuration {

	config, err := ioutil.ReadFile(*filename)
	if err != nil {
		fmt.Printf("failed to read config: ", err)
		os.Exit(1)
	}

	json.Unmarshal(config, &c)
	return c
}
