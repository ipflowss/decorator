package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"oracle/flowRecordDecorator/config"
	"os"
	"os/signal"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	DevicesFilename *string = kingpin.Flag("filename", "Devices File").Default("./data/deviceConfig.json").String()
	ConfigFilename  *string = kingpin.Flag("config", "Configuration File").Default("./config.json").String()
	log                     = logrus.New()
	devices         *DevicesContainer
	configLock      = new(sync.RWMutex)
)

type Event struct {
	EventType     string `json:"event_type, omitempty"`
	AsSrc         int    `json:"as_src, omitempty"`
	AsDst         int    `json:"as_dst, omitempty"`
	Comms         string `json:"comms, omitempty"`
	Ecomms        string `json:"ecomms, omitempty"`
	Lcomms        string `json:"lcomms, omitempty"`
	AsPath        string `json:"as_path, omitempty"`
	LocalPref     int    `json:"local_pref, omitempty"`
	Med           int    `json:"med, omitempty"`
	PeerAsDst     int    `json:"peer_as_dst, omitempty"`
	PeerIpSrc     string `json:"peer_ip_src, omitempty"`
	PeerIpDst     string `json:"peer_ip_dst, omitempty"`
	IfaceIn       int64  `json:"iface_in, omitempty"`
	IfaceOut      int64  `json:"iface_out, omitempty"`
	IpSrc         string `json:"ip_src, omitempty"`
	NetSrc        string `json:"net_src, omitempty"`
	IpDst         string `json:"ip_dst, omitempty"`
	NetDst        string `json:"net_dst, omitempty"`
	MaskSrc       int    `json:"mask_src, omitempty"`
	MaskDst       int    `json:"mask_dst, omitempty"`
	PortSrc       int    `json:"port_src, omitempty"`
	PortDst       int    `json:"port_dst, omitempty"`
	CountryIpSrc  string `json:"country_ip_src, omitempty"`
	CountryIPDst  string `json:"country_ip_dst, omitempty"`
	TcpFlags      string `json:"tcp_flags, omitempty"`
	IpProto       string `json:"ip_proto, omitempty"`
	Tos           int    `json:"tos, omitempty"`
	SamplingRate  int    `json:"sampling_rate, omitempty"`
	StampInserted string `json:"stamp_inserted, omitempty"`
	StampUpdated  string `json:"stamp_updated, omitempty"`
	Flows         int    `json:"flows, omitempty"`
	Packets       int    `json:"packets, omitempty"`
	Bytes         int    `json:"bytes, omitempty"`
	Pop           string `json:"pop, omitempty"`
	Region        string `json:"region, omitempty"`
	IfNameIn      string `json:"ifname_in, omitempty"`
	IfNameOut     string `json:"ifname_out, omitempty"`
	Hostname      string `json:"hostname, omitempty"`
	WriterId      string `json:"writer_id, omitempty"`
}

type DevicesContainer struct {
	Devices []Devices `json:"devices, omitempty"`
}

type Devices struct {
	Hostname   string       `json:"hostname, omitempty"`
	Region     string       `json:"region, omitempty"`
	Pop        string       `json:"pop, omitempty"`
	ExporterIP string       `json:"exporterip, omitempty"`
	Interfaces []Interfaces `json:"interfaces, omitempty"`
}

type Interfaces struct {
	IfName  string `json:"ifname, omitempty"`
	IfIndex int64  `json:"ifindex, omitempty"`
}

func loadDevicesConfig(fail bool) {
	file, err := ioutil.ReadFile(*DevicesFilename)
	if err != nil {
		log.Println("open config: ", err)
		if fail {
			os.Exit(1)
		}
	}

	temp := new(DevicesContainer)
	if err = json.Unmarshal(file, &temp); err != nil {
		log.Println("parse config: ", err)
		if fail {
			os.Exit(1)
		}
	}
	configLock.Lock()
	devices = temp
	configLock.Unlock()
}

func (c Devices) toString() string {
	return toJson(c)
}

func toJson(c interface{}) string {
	bytes, err := json.Marshal(c)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	return string(bytes)
}

func fromJson(b []byte, configuration config.Configuration) {
	var event Event

	loadDevicesConfig(true)

	err := json.Unmarshal(b, &event)
	if err != nil {
		// Not able to parse
		return
	}

	// identify the exporter IP
	for deviceIndex := range devices.Devices {
		if event.PeerIpSrc == devices.Devices[deviceIndex].ExporterIP {
			event.Hostname = devices.Devices[deviceIndex].Hostname
			event.Region = devices.Devices[deviceIndex].Region
			event.Pop = devices.Devices[deviceIndex].Pop
			for intIndex := range devices.Devices[deviceIndex].Interfaces {
				if event.IfaceIn == devices.Devices[deviceIndex].Interfaces[intIndex].IfIndex {
					event.IfNameIn = devices.Devices[deviceIndex].Interfaces[intIndex].IfName
				}
			}
		}
	}
	result, err := json.Marshal(event)
	if err != nil {
		// Not able to marshall
		return
	}
	producer(result, configuration)
	fmt.Println(string(result))
}

func consumer(configuration config.Configuration) {
	messageCountStart := configuration.MessageCountStart
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	brokers := configuration.BrokerList
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()
	fmt.Printf("IT! %s\n", *configuration.ConsumerTopic)
	consumer, err := master.ConsumePartition(*configuration.ConsumerTopic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	fmt.Printf("IT! %s\n", configuration.ConsumerTopic)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				messageCountStart++
				// fmt.Println("Received messages", string(msg.Key), string(msg.Value))
				fromJson(msg.Value, configuration)
			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
	fmt.Println("Processed", messageCountStart, "messages")
}

func producer(result []byte, configuration config.Configuration) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(configuration.BrokerList, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()
	msg := &sarama.ProducerMessage{
		Topic: configuration.ProducerTopic,
		Value: sarama.StringEncoder(result),
	}
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		panic(err)
	}
}

func main() {
	//Get any config flags
	kingpin.Parse()
	//Fetch a new Config Sutrct
	configuration := config.NewConfigHandle()
	//Parse the configuration that is in the Filename flag
	configuration = configuration.ParseConfig(ConfigFilename)

	consumer(configuration)
}
