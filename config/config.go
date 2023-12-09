package config

import (
	"github.com/caarlos0/env/v6"
	"github.com/rs/zerolog/log"
)

type common struct {
	Port             string `env:"PORT" envDefault:"3300"`
	FirstTimeFormat  string `env:"FIRST_TIME_FORMAT" envDefault:"02-Jan-2006T15:04:05"`
	SecondDateFormat string `env:"SECOND_TIME_FORMAT" envDefault:"02-Jan-2006"`
	SlackTimeFormat  string `env:"SLACK_TIME_FORMAT" envDefault:"15:04:05 MST 02 Jan 2006"`
	SlackWebhook     string `env:"SLACK_WEBHOOK"`
	Token            string `env:"TOKEN"`
	KafkaUrl         string `env:"KAFKA_URL"`
	// KafkaPort         string `env:"TOKEN"`
	KafkaUsername string `env:"KAFKA_USERNAME"`
	KafkaPassword string `env:"KAFKA_PASSWORD"`
}

var (
	Common common
)

func init() {
	env.Parse(&Common)

	log.Debug().Interface("common", Common).Send()

}
